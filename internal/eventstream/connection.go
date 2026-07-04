package eventstream

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/furconz/ed-iot-device-sdk-go/internal/logging"
)

// exitFunc is a package-level variable so tests can swap it without calling os.Exit.
var exitFunc = os.Exit

// nowFunc is a package-level variable so tests can inject a controllable clock.
var nowFunc = time.Now

// dropWindow is the sliding window duration for counting stream-level message drops.
const dropWindow = 30 * time.Second

// dropThreshold is the number of drops within dropWindow that triggers a self-restart.
const dropThreshold = 10

const (
	// A genuine orphan = a real command dropped on a wrongly-unregistered stream.
	// Kept LOW/wide so a persistently-deaf command subscription trips even under
	// sparse NFC-tap traffic (5/60s would never accumulate for a printer). One
	// spurious restart is cheap (bias to liveness); the persisted budget bounds loops.
	orphanWindow    = 10 * time.Minute
	orphanThreshold = 2
	closedIDTTL     = 30 * time.Second
	startupGrace    = 90 * time.Second

	// Connectivity flap/stuck detection (log-only reasons; the nucleus owns reconnection).
	flapWindow     = 5 * time.Minute
	flapThreshold  = 5
	stuckReconnect = 60 * time.Second

	ReasonRoutingMissOrphan = "routing-miss-orphan"
	ReasonStreamDrops       = "stream-drops"
	ReasonReconnectFlap     = "reconnect-flap"
	ReasonReconnectStuck    = "reconnect-stuck"
)

// markConnected records the time of a successful connect() as the startup-grace anchor.
func (c *Connection) markConnected() {
	c.healthMu.Lock()
	c.connectedAt = nowFunc()
	c.healthMu.Unlock()
}

// noteClosedID records that we cleanly closed stream id — used to discriminate QoS1
// redeliveries (a duplicate for a recently-closed id) from genuine routing-miss orphans.
// Must be called ONLY for ids we actually owned and closed (inside Close's ownership block).
func (c *Connection) noteClosedID(id uint32) {
	c.healthMu.Lock()
	if c.recentlyClosed == nil {
		c.recentlyClosed = make(map[uint32]time.Time)
	}
	c.recentlyClosed[id] = nowFunc()
	c.healthMu.Unlock()
}

// recordRoutingMiss runs on the readLoop goroutine (c.mu already RELEASED at the miss
// branch) for a NON-ZERO id that missed activeStreams. Counts the miss UNLESS it's a QoS1
// redelivery for a recently CLEANLY-closed id, past a startup grace, then signals.
// We deliberately do NOT require "never registered this generation": the deaf command
// stream's id WAS registered then wrongly unregistered, so a never-registered check would
// skip the exact bug. A wrongful unregister never records a clean-close, so
// not-recently-cleanly-closed is the correct discriminator.
//
// M2: recentlyClosed/connectedAt/orphanAt are also written by noteClosedID (from Close,
// under c.mu) and markConnected (from connect, under c.mu). This goroutine holds NEITHER
// lock here, so those fields are guarded by healthMu. We DECIDE under healthMu, then call
// suspectHealth OUTSIDE it (the callback must not run under healthMu; it does not re-enter
// the SDK). Lock order is always c.mu -> healthMu; recordRoutingMiss takes only healthMu,
// so there is no reverse ordering and no deadlock.
func (c *Connection) recordRoutingMiss(id uint32) {
	if id == 0 {
		return
	}
	c.healthMu.Lock()
	now := nowFunc()
	signal := false
	if now.Sub(c.connectedAt) >= startupGrace {
		if t, ok := c.recentlyClosed[id]; !ok || now.Sub(t) >= closedIDTTL {
			c.orphanAt = append(c.orphanAt, now)
			cutoff := now.Add(-orphanWindow)
			i := 0
			for i < len(c.orphanAt) && c.orphanAt[i].Before(cutoff) {
				i++
			}
			c.orphanAt = c.orphanAt[i:]
			if len(c.orphanAt) >= orphanThreshold {
				c.orphanAt = nil
				signal = true
			}
		}
	}
	c.healthMu.Unlock()
	if signal {
		c.suspectHealth(ReasonRoutingMissOrphan)
	}
}

// recordReconnectSuccess notes a successful reconnect for flap detection. flapThreshold
// reconnects within flapWindow → one ReasonReconnectFlap signal (log-only). Decides under
// healthMu, signals outside it.
func (c *Connection) recordReconnectSuccess() {
	c.healthMu.Lock()
	now := nowFunc()
	c.reconnectAt = append(c.reconnectAt, now)
	cutoff := now.Add(-flapWindow)
	i := 0
	for i < len(c.reconnectAt) && c.reconnectAt[i].Before(cutoff) {
		i++
	}
	c.reconnectAt = c.reconnectAt[i:]
	signal := false
	if len(c.reconnectAt) >= flapThreshold {
		c.reconnectAt = nil
		signal = true
	}
	c.healthMu.Unlock()
	if signal {
		c.suspectHealth(ReasonReconnectFlap)
	}
}

// suspectHealth routes a signal to the embedder's callback, or falls back to the SDK's
// own standalone behaviour when unset: exits (os.Stderr.Sync + exitFunc(1)) on the
// self-heal reasons (ReasonRoutingMissOrphan, ReasonStreamDrops), and logs only on the
// connectivity reasons (ReasonReconnectFlap, ReasonReconnectStuck) — exiting on a plain
// network outage would crash-loop an offline device, which the design explicitly forbids.
// Must NOT be called while holding healthMu.
func (c *Connection) suspectHealth(reason string) {
	logging.Error("eventstream: health signal (reason=%s)", reason)
	if c.config.OnHealthSignal != nil {
		c.config.OnHealthSignal(reason)
		return
	}
	// Standalone fallback: exit only on genuine deafness reasons.
	// Connectivity reasons (flap/stuck) represent a plain network outage —
	// the reconnect loop owns recovery; exiting here would crash-loop an offline device.
	switch reason {
	case ReasonRoutingMissOrphan, ReasonStreamDrops:
		os.Stderr.Sync() //nolint:errcheck
		exitFunc(1)
	default:
		// ReasonReconnectFlap / ReasonReconnectStuck: already logged above; no exit.
	}
}

// Connection represents an EventStream RPC connection
type Connection struct {
	conn   net.Conn
	config ConnectionConfig

	mu            sync.Mutex
	connected     bool
	nextStreamID  uint32
	activeStreams map[uint32]*Stream

	// For receiving messages
	readMu   sync.Mutex
	readErr  error
	incoming chan *Message

	// For sending messages
	writeMu sync.Mutex

	// For reconnection
	reconnectMu      sync.Mutex
	reconnecting     bool
	shouldReconnect  bool
	reconnectTrigger chan struct{}

	// For signaling connection ready state
	reconnectedChan chan struct{} // Closed when connection is ready
	reconnectedMu   sync.RWMutex  // Protects reconnectedChan

	// For ping/keepalive
	lastActivity time.Time
	lastPingSent time.Time
	lastPongRecv time.Time
	pingMu       sync.Mutex

	// droppedAt records the timestamps of stream-level message drops (stream.messages full).
	// Touched only by the single readLoop goroutine — no mutex required.
	// Survives reconnect because it is a field on *Connection, not on *Stream.
	droppedAt []time.Time

	// gen is a monotonically increasing counter bumped each time connect() succeeds.
	// Protected by mu. Subscriptions use it to detect when a new connection has been
	// established so they resubscribe exactly once per connection generation.
	gen uint64

	// healthMu guards the deafness/health-detector fields below. These are written by
	// noteClosedID (from Close, under c.mu) and markConnected (from connect, under c.mu),
	// and read/written by recordRoutingMiss (on the readLoop goroutine, holding NEITHER
	// c.mu nor readMu at the miss branch). Lock order is always c.mu -> healthMu.
	healthMu       sync.Mutex
	recentlyClosed map[uint32]time.Time // ids we cleanly closed → time closed (QoS1 discriminator)
	orphanAt       []time.Time          // timestamps of genuine routing-miss orphans (sliding window)
	connectedAt    time.Time            // last successful connect() — startup grace anchor
	reconnectAt    []time.Time          // timestamps of successful reconnects (flap sliding window)
}

// ConnectionConfig holds configuration for establishing a connection
type ConnectionConfig struct {
	// SocketPath is the path to the Unix domain socket
	SocketPath string

	// AuthToken is the authentication token for the connection
	AuthToken string

	// EnableReconnection enables automatic reconnection on connection failure
	EnableReconnection bool

	// DisablePingPong disables proactive keepalive pings
	DisablePingPong bool

	// PingInterval is how often to send keepalive pings
	PingInterval time.Duration

	// PingTimeout is how long to wait for ping response
	PingTimeout time.Duration

	// MaxRetries is max retry attempts for request-response operations
	MaxRetries int

	// OnDisconnected callback when connection is lost
	OnDisconnected func(error)

	// OnReconnected callback when connection is restored
	OnReconnected func()

	// OnHealthSignal is called when the SDK detects a health-relevant signal
	// (genuine routing-miss orphan, sustained stream drops, reconnect flap/stuck).
	// If nil, the SDK falls back to standalone behaviour: exits (exitFunc(1)) on the
	// self-heal reasons (ReasonRoutingMissOrphan, ReasonStreamDrops) and logs only on
	// the connectivity reasons (ReasonReconnectFlap, ReasonReconnectStuck).
	OnHealthSignal func(reason string)
}

// Stream represents an EventStream RPC operation stream
type Stream struct {
	id        uint32
	conn      *Connection
	operation string

	mu         sync.Mutex
	active     bool
	closed     bool // tracks if done channel has been closed
	idAllocated bool // tracks if stream ID has been allocated (set in Activate)
	messages   chan *Message
	errors     chan error
	done       chan struct{}
}

// Connect establishes a new EventStream RPC connection
func Connect(ctx context.Context, config ConnectionConfig) (*Connection, error) {
	// Create open channel - will be closed in connect() when ready
	readyChan := make(chan struct{})

	c := &Connection{
		config:           config,
		nextStreamID:     1, // Stream ID 0 is reserved for connection-level messages
		activeStreams:    make(map[uint32]*Stream),
		incoming:         make(chan *Message, 100),
		shouldReconnect:  config.EnableReconnection,
		reconnectTrigger: make(chan struct{}, 1),
		reconnectedChan:  readyChan,
		lastActivity:     time.Now(),
		lastPongRecv:     time.Now(),
	}

	// Perform initial connection
	if err := c.connect(ctx); err != nil {
		return nil, err
	}

	// Start reconnection loop if enabled
	if config.EnableReconnection {
		go c.reconnectLoop(ctx)
	}

	return c, nil
}

// connect performs the actual socket connection and handshake
func (c *Connection) connect(ctx context.Context) error {
	logging.Info("Connecting to socket: %s", c.config.SocketPath)

	// Connect to Unix domain socket
	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(ctx, "unix", c.config.SocketPath)
	if err != nil {
		return &ConnectionError{Err: fmt.Errorf("failed to connect to socket: %w", err)}
	}
	logging.Info("Socket connected successfully")

	c.conn = conn

	// Perform handshake
	if err := c.handshake(ctx); err != nil {
		conn.Close()
		return fmt.Errorf("handshake failed: %w", err)
	}

	c.mu.Lock()
	c.connected = true
	c.nextStreamID = 1
	c.activeStreams = make(map[uint32]*Stream)
	c.gen++
	c.mu.Unlock()

	// Anchor the startup grace for the routing-miss orphan detector (per-connect).
	c.markConnected()

	c.readMu.Lock()
	c.readErr = nil
	c.incoming = make(chan *Message, 100)
	c.readMu.Unlock()

	c.pingMu.Lock()
	c.lastActivity = time.Now()
	c.lastPongRecv = time.Now()
	c.pingMu.Unlock()

	// Signal that connection is ready for use
	c.reconnectedMu.Lock()
	if c.reconnectedChan != nil {
		select {
		case <-c.reconnectedChan:
			// Already closed, skip
			logging.Debug("Connection ready channel already closed")
		default:
			// Channel is open, safe to close
			close(c.reconnectedChan)
		}
	}
	c.reconnectedMu.Unlock()

	// Start message reader
	go c.readLoop()

	// Start ping loop if keepalive enabled and not disabled
	if c.config.PingInterval > 0 && !c.config.DisablePingPong {
		go c.pingLoop()
		logging.Debug("Ping loop enabled (interval: %v, timeout: %v)", c.config.PingInterval, c.config.PingTimeout)
	} else if c.config.DisablePingPong {
		logging.Debug("Ping loop disabled by configuration")
	}

	logging.Info("Connection established successfully")

	return nil
}

// reconnectLoop handles automatic reconnection with exponential backoff
func (c *Connection) reconnectLoop(ctx context.Context) {
	logging.Debug("Reconnection loop started")
	for {
		// Wait for reconnection trigger
		select {
		case <-ctx.Done():
			logging.Debug("Reconnection loop exiting due to context cancellation")
			return
		case <-c.reconnectTrigger:
			// Connection lost, attempt reconnection
		}

		c.reconnectMu.Lock()
		if !c.shouldReconnect {
			c.reconnectMu.Unlock()
			logging.Debug("Reconnection disabled, exiting reconnect loop")
			return
		}
		c.reconnecting = true
		c.reconnectMu.Unlock()

		logging.Info("Starting reconnection attempts...")

		// Exponential backoff: 1s, 2s, 4s, 8s, 16s, 30s (max)
		delay := 1 * time.Second
		maxDelay := 30 * time.Second
		attempt := 1

		// Stuck-reconnect detection: if a SINGLE reconnect episode spends longer than
		// stuckReconnect without succeeding, signal ReasonReconnectStuck once (log-only).
		reconnectStart := nowFunc()
		stuckSignaled := false

		for {
			select {
			case <-ctx.Done():
				c.reconnectMu.Lock()
				c.reconnecting = false
				c.reconnectMu.Unlock()
				return
			default:
			}

			logging.Info("Reconnection attempt %d (waiting %v)...", attempt, delay)
			time.Sleep(delay)

			// Attempt to reconnect
			err := c.connect(ctx)
			if err == nil {
				// Successfully reconnected
				logging.Info("Reconnection successful after %d attempts", attempt)

				c.reconnectMu.Lock()
				c.reconnecting = false
				c.reconnectMu.Unlock()

				// Flap detection: flapThreshold reconnects within flapWindow → signal.
				c.recordReconnectSuccess()

				// Call reconnected callback
				if c.config.OnReconnected != nil {
					go c.config.OnReconnected()
				}

				break // Exit retry loop, wait for next trigger
			}

			logging.Error("Reconnection attempt %d failed: %v", attempt, err)

			// Stuck detection: fire once per episode when time-in-reconnect crosses the bound.
			if !stuckSignaled && nowFunc().Sub(reconnectStart) > stuckReconnect {
				stuckSignaled = true
				c.suspectHealth(ReasonReconnectStuck)
			}

			// Increase delay with exponential backoff
			delay = delay * 2
			if delay > maxDelay {
				delay = maxDelay
			}
			attempt++
		}
	}
}

// TriggerReconnect is the exported entry point for forcing a reconnect from an
// embedder (e.g. Client.ForceReconnect for test/ops). It delegates to triggerReconnect.
func (c *Connection) TriggerReconnect(err error) {
	c.triggerReconnect(err)
}

// triggerReconnect signals the reconnection loop to start reconnecting
func (c *Connection) triggerReconnect(err error) {
	c.reconnectMu.Lock()
	shouldTrigger := c.shouldReconnect && !c.reconnecting
	c.reconnectMu.Unlock()

	if !shouldTrigger {
		logging.Debug("Reconnection not triggered (disabled or already reconnecting)")
		return
	}

	logging.Info("Triggering reconnection due to: %v", err)

	// Call disconnected callback
	if c.config.OnDisconnected != nil {
		go c.config.OnDisconnected(err)
	}

	// Non-blocking trigger
	select {
	case c.reconnectTrigger <- struct{}{}:
	default:
		// Already triggered, no need to send again
	}
}

// WaitUntilReady blocks until the connection is ready for use
// Returns nil when connection is ready, or context error if cancelled
func (c *Connection) WaitUntilReady(ctx context.Context) error {
	c.reconnectedMu.RLock()
	ch := c.reconnectedChan
	c.reconnectedMu.RUnlock()

	if ch == nil {
		// Channel not initialized, connection is ready
		return nil
	}

	select {
	case <-ch:
		// Connection is ready
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// pingLoop sends periodic keepalive pings and checks for stale connections
func (c *Connection) pingLoop() {
	logging.Debug("Ping loop started (interval: %v, timeout: %v)", c.config.PingInterval, c.config.PingTimeout)

	ticker := time.NewTicker(c.config.PingInterval)
	defer ticker.Stop()

	for {
		<-ticker.C

		// Check if connection is still active
		c.mu.Lock()
		connected := c.connected
		c.mu.Unlock()

		if !connected {
			logging.Debug("Ping loop exiting: connection not active")
			return
		}

		c.pingMu.Lock()
		timeSinceActivity := time.Since(c.lastActivity)
		lastPingSent := c.lastPingSent
		lastPongRecv := c.lastPongRecv
		c.pingMu.Unlock()

		// Only send ping if we've been idle
		if timeSinceActivity < c.config.PingInterval {
			logging.Debug("Skipping ping: recent activity (%v ago)", timeSinceActivity)
			continue
		}

		// Check if we have an outstanding ping that timed out
		if !lastPingSent.IsZero() && lastPingSent.After(lastPongRecv) {
			// We sent a ping but haven't received pong yet
			timeSincePing := time.Since(lastPingSent)

			if timeSincePing > c.config.PingTimeout {
				logging.Error("Ping timeout: no pong received for %v after sending ping (timeout: %v)",
					timeSincePing, c.config.PingTimeout)
				// Close connection to trigger reconnection
				if c.conn != nil {
					c.conn.Close()
				}
				return
			}

			// Ping still pending, don't send another yet
			logging.Debug("Ping already outstanding for %v, waiting for pong", timeSincePing)
			continue
		}

		// Send new ping
		logging.Debug("Sending keepalive ping...")
		pingMsg := CreateMessage(MessageTypePing, MessageFlagNone, nil)

		c.pingMu.Lock()
		c.lastPingSent = time.Now()
		c.pingMu.Unlock()

		if err := c.writeMessage(pingMsg); err != nil {
			logging.Error("Failed to send ping: %v", err)
			// Write error will be caught by readLoop or next operation
			return
		}

		logging.Debug("Keepalive ping sent")
	}
}

// handshake performs the CONNECT/CONNACK handshake
func (c *Connection) handshake(ctx context.Context) error {
	// Create CONNECT message
	connectReq := ConnectRequest{
		AuthToken: c.config.AuthToken,
	}
	payload, err := json.Marshal(connectReq)
	if err != nil {
		return fmt.Errorf("failed to marshal connect request: %w", err)
	}

	connectMsg := CreateMessage(MessageTypeConnect, MessageFlagNone, payload)
	connectMsg.SetHeader(":version", HeaderTypeString, "0.1.0")

	logging.Debug("Sending CONNECT message: type=%v flags=%v headers=%v payloadLen=%d",
		connectMsg.Type, connectMsg.Flags, len(connectMsg.Headers), len(connectMsg.Payload))

	for i, h := range connectMsg.Headers {
		logging.Debug("Sending Header[%d]: name=%s type=%d value=%v", i, h.Name, h.Type, h.Value)
	}

	// Send CONNECT
	if err := c.writeMessage(connectMsg); err != nil {
		return fmt.Errorf("failed to send CONNECT: %w", err)
	}

	logging.Debug("CONNECT sent, waiting for CONNACK...")

	// Read CONNACK with timeout
	connackChan := make(chan *Message, 1)
	errChan := make(chan error, 1)

	go func() {
		msg, err := DecodeMessage(c.conn)
		if err != nil {
			errChan <- err
			return
		}
		connackChan <- msg
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errChan:
		return fmt.Errorf("failed to read CONNACK: %w", err)
	case msg := <-connackChan:
		logging.Debug("Received message: type=%v (%d) flags=%v headers=%v payloadLen=%d",
			msg.Type, uint32(msg.Type), msg.Flags, len(msg.Headers), len(msg.Payload))

		for i, h := range msg.Headers {
			logging.Debug("Header[%d]: name=%s type=%d value=%v", i, h.Name, h.Type, h.Value)
		}

		if len(msg.Payload) > 0 {
			logging.Debug("Payload: %s", string(msg.Payload))
		}

		if msg.Type == MessageTypeProtocolError || msg.Type == MessageTypeInternalError {
			errorType, _ := msg.GetStringHeader("service-model-type")
			contentType, _ := msg.GetStringHeader(":content-type")
			errorMsg := string(msg.Payload)
			logging.Error("Handshake failed - %v from server", msg.Type)
			logging.Error("  Error Type: %s", errorType)
			logging.Error("  Content Type: %s", contentType)
			logging.Error("  Payload: %s", errorMsg)
			return fmt.Errorf("%v from server: type=%s contentType=%s message=%s", msg.Type, errorType, contentType, errorMsg)
		}

		if msg.Type != MessageTypeConnectAck {
			return fmt.Errorf("expected CONNACK, got %v", msg.Type)
		}
		if !msg.Flags.HasFlag(MessageFlagConnectionAccepted) {
			return fmt.Errorf("connection not accepted")
		}
	}

	return nil
}

// readLoop continuously reads messages from the connection
func (c *Connection) readLoop() {
	for {
		msg, err := DecodeMessage(c.conn)
		if err != nil {
			logging.Error("DecodeMessage error: %v", err)

			// Wrap as connection error if it's a network error
			wrappedErr := WrapIfConnectionError(err)

			c.readMu.Lock()
			c.readErr = wrappedErr
			c.readMu.Unlock()

			// Create new reconnection signal BEFORE marking disconnected
			// This ensures operations that check readiness will block until reconnection succeeds
			c.reconnectedMu.Lock()
			c.reconnectedChan = make(chan struct{}) // Open channel = not ready
			c.reconnectedMu.Unlock()

			// Mark connection as not connected
			c.mu.Lock()
			c.connected = false

			// Close all active streams
			for _, stream := range c.activeStreams {
				select {
				case stream.errors <- wrappedErr:
				default:
				}
				stream.mu.Lock()
				if !stream.closed {
					close(stream.done)
					stream.closed = true
				}
				stream.mu.Unlock()
			}
			c.mu.Unlock()

			close(c.incoming)

			// Trigger reconnection if enabled and it's a connection error
			if IsConnectionError(wrappedErr) {
				c.triggerReconnect(wrappedErr)
			}

			return
		}

		// Update last activity time
		c.pingMu.Lock()
		c.lastActivity = time.Now()
		c.pingMu.Unlock()

		// Log EVERY message received before any processing
		logging.Debug("Raw message received: type=%s flags=%s payloadLen=%d headerCount=%d",
			msg.Type, msg.Flags, len(msg.Payload), len(msg.Headers))

		// Extract stream ID from message headers to route it correctly
		streamID := uint32(0)
		for _, h := range msg.Headers {
			if h.Name == ":stream-id" {
				if v, ok := h.Value.(int32); ok {
					streamID = uint32(v)
				}
			}
		}

		logging.Debug("Received message for stream %d: type=%s flags=%s",
			streamID, msg.Type, msg.Flags)

		// Handle ping/pong messages
		if msg.Type == MessageTypePing {
			logging.Debug("Received ping, sending pong...")
			pongMsg := CreateMessage(MessageTypePingResponse, MessageFlagNone, nil)
			if err := c.writeMessage(pongMsg); err != nil {
				logging.Error("Failed to send pong: %v", err)
			} else {
				logging.Debug("Pong sent")
			}
			continue
		}

		if msg.Type == MessageTypePingResponse {
			c.pingMu.Lock()
			c.lastPongRecv = time.Now()
			c.pingMu.Unlock()
			logging.Debug("Received pong")
			continue
		}

		// Route message to appropriate stream
		c.mu.Lock()
		stream, exists := c.activeStreams[streamID]
		c.mu.Unlock()

		if exists {
			// Check for error response FIRST (before checking termination flag)
			// This ensures we log the error even if TERMINATE_STREAM flag is also set
			if msg.Type == MessageTypeApplicationError {
				err := c.parseErrorMessage(msg)
				logging.Error("Stream %d received ApplicationError: %v", streamID, err)
				logging.Error("  Payload: %s", string(msg.Payload))
				select {
				case stream.errors <- err:
				default:
				}
				// Don't continue yet - check if we also need to close the stream
			}

			// Deliver message BEFORE checking termination flag
			// For request-response, the response has TERMINATE_STREAM flag but still contains the response payload
			if msg.Type != MessageTypeApplicationError {
				logging.Debug("Routing message to stream %d (payloadLen=%d)", streamID, len(msg.Payload))
				select {
				case stream.messages <- msg:
				case <-stream.done:
					// Stream closed, ignore message
					logging.Info("Stream %d already closed, dropping message", streamID)
				default:
					logging.Error("Stream %d message channel full, dropping message", streamID)
					c.recordStreamDrop()
				}
			}

			// NOW check if this is a termination message (after delivering the message)
			if msg.Flags.HasFlag(MessageFlagTerminateStream) {
				logging.Debug("Stream %d received TERMINATE_STREAM (after message delivery)", streamID)
				stream.mu.Lock()
				if !stream.closed {
					close(stream.done)
					stream.closed = true
				}
				stream.mu.Unlock()
			}
		} else {
			// No stream registered for this id. For a non-zero id this is a routing
			// miss — a real frame arrived for a stream that is not in activeStreams.
			// recordRoutingMiss discriminates genuine orphans (wrongly-unregistered,
			// RUNNING-but-deaf) from benign QoS1 redeliveries of recently-closed ids.
			// Zero-id (connection-level) messages are ignored inside recordRoutingMiss.
			c.recordRoutingMiss(streamID)

			// No specific stream, send to incoming channel (connection-level messages)
			if msg.Type == MessageTypeProtocolError || msg.Type == MessageTypeInternalError {
				// Log connection-level errors
				logging.Error("Connection-level error received:")
				logging.Error("  Type: %s", msg.Type)
				logging.Error("  Payload: %s", string(msg.Payload))
			}
			select {
			case c.incoming <- msg:
			default:
				logging.Error("Incoming channel full, dropping message")
			}
		}
	}
}

// writeMessage sends a message on the connection
func (c *Connection) writeMessage(msg *Message) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	encoded, err := EncodeMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to encode message: %w", err)
	}

	logging.Debug("Writing %d bytes to socket", len(encoded))
	if len(encoded) < 200 {
		logging.Debug("Wire bytes: % x", encoded)
	}

	if _, err := c.conn.Write(encoded); err != nil {
		wrappedErr := WrapIfConnectionError(err)
		// Trigger reconnection if it's a connection error
		if IsConnectionError(wrappedErr) {
			c.triggerReconnect(wrappedErr)
		}
		return fmt.Errorf("failed to write message: %w", wrappedErr)
	}

	// Update last activity time on successful write
	c.pingMu.Lock()
	c.lastActivity = time.Now()
	c.pingMu.Unlock()

	return nil
}

// Generation returns the current connection generation counter, which is
// incremented each time connect() completes successfully. Subscriptions use
// this to ensure resubscription fires exactly once per connection generation.
func (c *Connection) Generation() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gen
}

// StreamOwner returns the stream registered at id in the activeStreams routing map,
// along with a boolean indicating whether an entry exists. Used by the D1a tripwire
// in greengrassipc to assert that a freshly-resubscribed stream is the registered owner
// of its id (pointer comparison). Reads activeStreams under c.mu.
func (c *Connection) StreamOwner(id uint32) (*Stream, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.activeStreams[id]
	return s, ok
}

// Close closes the connection
func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.connected {
		return nil
	}

	c.connected = false

	// Close all active streams
	for _, stream := range c.activeStreams {
		close(stream.done)
	}
	c.activeStreams = nil

	return c.conn.Close()
}

// NewStream creates a new operation stream
// Note: Stream ID is not allocated until Activate() is called, to ensure
// atomic allocation + first message send and prevent stream-id ordering violations
func (c *Connection) NewStream(operation string) *Stream {
	stream := &Stream{
		id:          0, // Will be allocated in Activate()
		conn:        c,
		operation:   operation,
		idAllocated: false,
		messages:    make(chan *Message, 10),
		errors:      make(chan error, 1),
		done:        make(chan struct{}),
	}

	logging.Debug("Created stream for operation: %s (ID will be allocated on activation)", operation)

	return stream
}

// RequestResponse performs a simple request-response operation with automatic retry on connection failure
func (c *Connection) RequestResponse(ctx context.Context, operation string, request interface{}) ([]byte, error) {
	maxRetries := c.config.MaxRetries
	if maxRetries == 0 {
		maxRetries = 1 // At least one attempt
	}

	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Check if context is cancelled
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Wait for connection to be ready if we're reconnecting
		if attempt > 1 {
			logging.Info("Request-response retry attempt %d/%d for operation %s - waiting for connection...", attempt, maxRetries, operation)

			// Use WaitUntilReady with a timeout
			waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			err := c.WaitUntilReady(waitCtx)
			cancel()

			if err != nil {
				logging.Error("Connection not ready for retry attempt %d: %v", attempt, err)
				continue
			}

			logging.Debug("Connection ready for retry attempt %d", attempt)
		}

		result, err := c.requestResponseOnce(ctx, operation, request)
		if err == nil {
			if attempt > 1 {
				logging.Info("Request-response succeeded on retry attempt %d", attempt)
			}
			return result, nil
		}

		lastErr = err

		// Only retry on connection errors
		if !IsConnectionError(err) {
			logging.Debug("Not retrying non-connection error: %v", err)
			return nil, err
		}

		logging.Error("Request-response attempt %d failed with connection error: %v", attempt, err)

		// Don't sleep after last attempt
		if attempt < maxRetries {
			// Wait a bit before retrying
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Second):
				// Short delay between retries
			}
		}
	}

	return nil, fmt.Errorf("request-response failed after %d attempts: %w", maxRetries, lastErr)
}

// requestResponseOnce performs a single request-response operation without retrylogic
func (c *Connection) requestResponseOnce(ctx context.Context, operation string, request interface{}) ([]byte, error) {
	// Create a new stream for this request-response operation
	stream := c.NewStream(operation)

	// Activate the stream with the request
	if err := stream.Activate(ctx, request); err != nil {
		return nil, fmt.Errorf("failed to activate stream: %w", err)
	}
	defer stream.Close()

	// Wait for response message
	// Note: We don't check stream.Done() here because the server sends TERMINATE_STREAM
	// flag on the response for request-response operations, which is normal behavior
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-stream.Errors():
		if err != nil {
			return nil, fmt.Errorf("stream error: %w", err)
		}
		// Errors channel closed, check for response
		select {
		case respMsg := <-stream.Messages():
			if respMsg == nil {
				return nil, fmt.Errorf("stream closed without response")
			}
			return c.handleResponseMessage(respMsg)
		default:
			return nil, fmt.Errorf("stream closed without response")
		}
	case respMsg := <-stream.Messages():
		if respMsg == nil {
			return nil, fmt.Errorf("stream closed without response")
		}
		return c.handleResponseMessage(respMsg)
	}
}

func (c *Connection) handleResponseMessage(respMsg *Message) ([]byte, error) {
	// Check for error response
	if respMsg.Type == MessageTypeApplicationError {
		err := c.parseErrorMessage(respMsg)
		logging.Error("Request-response operation failed: %v", err)
		logging.Error("  Payload: %s", string(respMsg.Payload))
		return nil, err
	}

	if respMsg.Type != MessageTypeApplicationMessage {
		return nil, fmt.Errorf("unexpected message type: %v", respMsg.Type)
	}

	// Success - return the response payload
	// (The TERMINATE_STREAM flag on the response is normal for request-response operations)
	return respMsg.Payload, nil
}

// parseErrorMessage parses an error message into an OperationError
func (c *Connection) parseErrorMessage(msg *Message) error {
	serviceModelType, _ := msg.GetStringHeader("service-model-type")
	contentType, _ := msg.GetStringHeader(":content-type")

	opErr := &OperationError{
		ErrorType:      serviceModelType,
		IsServiceError: true,
	}

	// If content-type is text/plain, the payload is a plain text error message
	if contentType == "text/plain" {
		opErr.ErrorMessage = string(msg.Payload)
		opErr.IsInternalError = true
	} else {
		// Otherwise, it's a JSON-encoded modeled error
		opErr.ModeledError = msg.Payload

		// Try to extract error message from common fields
		var errMap map[string]interface{}
		if err := json.Unmarshal(msg.Payload, &errMap); err == nil {
			if errMsg, ok := errMap["message"].(string); ok {
				opErr.ErrorMessage = errMsg
			} else if errMsg, ok := errMap["errorMessage"].(string); ok {
				opErr.ErrorMessage = errMsg
			}
		}
	}

	return opErr
}

// Activate activates a stream by sending the initial request
func (s *Stream) Activate(ctx context.Context, request interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.active {
		return fmt.Errorf("stream already active")
	}

	// Serialize request
	payload, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	// CRITICAL: Allocate stream ID and send first message atomically under connection lock
	// This prevents race conditions where stream N+1 sends before stream N
	s.conn.mu.Lock()

	// Allocate stream ID if not already allocated
	if !s.idAllocated {
		s.id = s.conn.nextStreamID
		s.conn.nextStreamID++
		s.idAllocated = true
		s.conn.activeStreams[s.id] = s
		logging.Debug("Allocated stream ID %d for operation: %s", s.id, s.operation)
	}

	logging.Debug("Activating stream %d:", s.id)
	logging.Debug("  Operation: %s", s.operation)
	logging.Debug("  Request payload: %s", string(payload))

	// Create application message with stream ID
	msg := CreateMessageWithStreamID(MessageTypeApplicationMessage, MessageFlagNone, s.id, payload)
	msg.SetHeader("operation", HeaderTypeString, s.operation)

	// Send request (still under connection lock for atomicity)
	err = s.conn.writeMessage(msg)

	s.conn.mu.Unlock()

	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	s.active = true
	logging.Debug("Stream %d activated successfully", s.id)

	return nil
}

// Messages returns the channel for receiving messages
func (s *Stream) Messages() <-chan *Message {
	return s.messages
}

// Errors returns the channel for receiving errors
func (s *Stream) Errors() <-chan error {
	return s.errors
}

// Done returns a channel that is closed when the stream is done
func (s *Stream) Done() <-chan struct{} {
	return s.done
}

// ID returns the numeric stream ID allocated during Activate.
// Returns 0 before Activate is called.
func (s *Stream) ID() uint32 {
	return s.id
}

// Close closes the stream
func (s *Stream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.active {
		return nil
	}

	logging.Debug("Closing stream %d (operation: %s)", s.id, s.operation)

	// CRITICAL: Hold connection mutex during TERMINATE send to ensure stream-id monotonicity
	// We also remove from activeStreams in the same critical section for efficiency
	s.conn.mu.Lock()

	// Only send TERMINATE if:
	// 1. Stream ID was allocated (stream was activated)
	// 2. Stream wasn't already closed by readLoop
	if s.idAllocated && !s.closed {
		msg := CreateMessageWithStreamID(MessageTypeApplicationMessage, MessageFlagTerminateStream, s.id, nil)
		if err := s.conn.writeMessage(msg); err != nil {
			logging.Error("Failed to send TERMINATE for stream %d: %v", s.id, err)
		} else {
			logging.Debug("Sent TERMINATE for stream %d", s.id)
		}
		close(s.done)
		s.closed = true
	} else if !s.idAllocated {
		logging.Debug("Stream not activated, skipping TERMINATE")
		if !s.closed {
			close(s.done)
			s.closed = true
		}
	} else {
		logging.Debug("Stream %d already closed by readLoop, skipping TERMINATE", s.id)
	}

	// Remove from active streams only if THIS stream is still the registered
	// owner of its id. After a reconnect resets nextStreamID and installs a
	// fresh map, a stale stream's id may have been reused by a live stream —
	// an unconditional delete would silently unregister that live subscription
	// (RUNNING-but-deaf). See connection_p0_test.go.
	if s.idAllocated {
		if cur, ok := s.conn.activeStreams[s.id]; ok && cur == s {
			delete(s.conn.activeStreams, s.id)
			// Record a CLEAN close so a QoS1 redelivery for this id is not mistaken
			// for a genuine routing-miss orphan. Only inside the ownership block: a
			// wrongful/no-op delete must NOT poison the recently-closed set, or it
			// would suppress detection of the exact deafness bug. (Under c.mu here;
			// noteClosedID takes healthMu — lock order c.mu -> healthMu is preserved.)
			s.conn.noteClosedID(s.id)
		}
	}

	s.conn.mu.Unlock()

	s.active = false

	logging.Debug("Stream %d closed and removed from active streams", s.id)

	return nil
}

// recordStreamDrop records a stream-level message drop (stream.messages channel full)
// and triggers a self-restart via exitFunc(1) if dropThreshold drops occur within dropWindow.
//
// Must only be called from the readLoop goroutine — no mutex is needed because droppedAt
// is a field on *Connection (survives reconnect) and is accessed from a single goroutine.
func (c *Connection) recordStreamDrop() {
	now := nowFunc()
	c.droppedAt = append(c.droppedAt, now)

	// Evict entries that have aged out of the sliding window.
	cutoff := now.Add(-dropWindow)
	i := 0
	for i < len(c.droppedAt) && c.droppedAt[i].Before(cutoff) {
		i++
	}
	c.droppedAt = c.droppedAt[i:]

	if len(c.droppedAt) >= dropThreshold {
		logging.Error(
			"eventstream: %d inbound messages dropped within %s — consumer stalled; signalling health",
			len(c.droppedAt),
			dropWindow,
		)
		// Reset so a subsequent breach signals afresh rather than re-firing every drop.
		c.droppedAt = nil
		// Route through the health path: the embedder's OnHealthSignal decides how to
		// self-heal, or the SDK falls back to exitFunc(1) (which flushes stderr) standalone.
		c.suspectHealth(ReasonStreamDrops)
	}
}
