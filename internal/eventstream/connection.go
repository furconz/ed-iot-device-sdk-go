package eventstream

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"

	"github.com/furconz/ed-iot-device-sdk-go/internal/logging"
)

// Connection represents an EventStream RPC connection
type Connection struct {
	conn      net.Conn
	authToken string

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
}

// ConnectionConfig holds configuration for establishing a connection
type ConnectionConfig struct {
	// SocketPath is the path to the Unix domain socket
	SocketPath string

	// AuthToken is the authentication token for the connection
	AuthToken string
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
	logging.Info("Connecting to socket: %s", config.SocketPath)
	// Connect to Unix domain socket
	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(ctx, "unix", config.SocketPath)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to socket: %w", err)
	}
	logging.Info("Socket connected successfully")

	c := &Connection{
		conn:          conn,
		authToken:     config.AuthToken,
		nextStreamID:  1, // Stream ID 0 is reserved for connection-level messages
		activeStreams: make(map[uint32]*Stream),
		incoming:      make(chan *Message, 100),
	}

	// Perform handshake
	if err := c.handshake(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("handshake failed: %w", err)
	}

	c.connected = true

	// Start message reader
	go c.readLoop()

	return c, nil
}

// handshake performs the CONNECT/CONNACK handshake
func (c *Connection) handshake(ctx context.Context) error {
	// Create CONNECT message
	connectReq := ConnectRequest{
		AuthToken: c.authToken,
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
			c.readMu.Lock()
			c.readErr = err
			c.readMu.Unlock()

			// Close all active streams
			c.mu.Lock()
			for _, stream := range c.activeStreams {
				select {
				case stream.errors <- err:
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
			return
		}

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
		return fmt.Errorf("failed to write message: %w", err)
	}

	return nil
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

// RequestResponse performs a simple request-response operation
func (c *Connection) RequestResponse(ctx context.Context, operation string, request interface{}) ([]byte, error) {
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

	// Remove from active streams only if ID was allocated
	if s.idAllocated {
		delete(s.conn.activeStreams, s.id)
	}

	s.conn.mu.Unlock()

	s.active = false

	logging.Debug("Stream %d closed and removed from active streams", s.id)

	return nil
}
