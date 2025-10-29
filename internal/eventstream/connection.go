package eventstream

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
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

	mu       sync.Mutex
	active   bool
	messages chan *Message
	errors   chan error
	done     chan struct{}
}

// Connect establishes a new EventStream RPC connection
func Connect(ctx context.Context, config ConnectionConfig) (*Connection, error) {
	// Connect to Unix domain socket
	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(ctx, "unix", config.SocketPath)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to socket: %w", err)
	}

	c := &Connection{
		conn:          conn,
		authToken:     config.AuthToken,
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

	// Send CONNECT
	if err := c.writeMessage(connectMsg); err != nil {
		return fmt.Errorf("failed to send CONNECT: %w", err)
	}

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
				close(stream.done)
			}
			c.mu.Unlock()

			close(c.incoming)
			return
		}

		// Route message to appropriate stream or incoming channel
		// For now, just send to incoming channel
		// In a full implementation, we'd need stream IDs to route messages
		select {
		case c.incoming <- msg:
		default:
			// Channel full, log warning
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
func (c *Connection) NewStream(operation string) *Stream {
	c.mu.Lock()
	defer c.mu.Unlock()

	streamID := c.nextStreamID
	c.nextStreamID++

	stream := &Stream{
		id:        streamID,
		conn:      c,
		operation: operation,
		messages:  make(chan *Message, 10),
		errors:    make(chan error, 1),
		done:      make(chan struct{}),
	}

	c.activeStreams[streamID] = stream

	return stream
}

// RequestResponse performs a simple request-response operation
func (c *Connection) RequestResponse(ctx context.Context, operation string, request interface{}) ([]byte, error) {
	// Serialize request
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Create application message
	msg := CreateMessage(MessageTypeApplicationMessage, MessageFlagNone, payload)
	msg.SetHeader("service-model-type", HeaderTypeString, operation)

	// Send request
	if err := c.writeMessage(msg); err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	// Wait for response
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case respMsg := <-c.incoming:
		if respMsg == nil {
			c.readMu.Lock()
			err := c.readErr
			c.readMu.Unlock()
			if err != nil && err != io.EOF {
				return nil, fmt.Errorf("connection error: %w", err)
			}
			return nil, fmt.Errorf("connection closed")
		}

		// Check for error response
		if respMsg.Type == MessageTypeApplicationError {
			return nil, c.parseErrorMessage(respMsg)
		}

		if respMsg.Type != MessageTypeApplicationMessage {
			return nil, fmt.Errorf("unexpected message type: %v", respMsg.Type)
		}

		return respMsg.Payload, nil
	}
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

	// Create application message
	msg := CreateMessage(MessageTypeApplicationMessage, MessageFlagNone, payload)
	msg.SetHeader("service-model-type", HeaderTypeString, s.operation)

	// Send request
	if err := s.conn.writeMessage(msg); err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	s.active = true

	// Start receiving messages for this stream
	go s.receiveLoop()

	return nil
}

// receiveLoop receives messages for this stream
func (s *Stream) receiveLoop() {
	for {
		select {
		case <-s.done:
			return
		case msg := <-s.conn.incoming:
			if msg == nil {
				// Connection closed
				select {
				case s.errors <- fmt.Errorf("connection closed"):
				default:
				}
				return
			}

			// Check if this is a termination message
			if msg.Flags.HasFlag(MessageFlagTerminateStream) {
				close(s.done)
				return
			}

			// Check for error response
			if msg.Type == MessageTypeApplicationError {
				err := s.conn.parseErrorMessage(msg)
				select {
				case s.errors <- err:
				default:
				}
				continue
			}

			// Send to messages channel
			select {
			case s.messages <- msg:
			case <-s.done:
				return
			}
		}
	}
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

	// Send termination message
	msg := CreateMessage(MessageTypeApplicationMessage, MessageFlagTerminateStream, nil)
	if err := s.conn.writeMessage(msg); err != nil {
		// Best effort
	}

	close(s.done)
	s.active = false

	// Remove from active streams
	s.conn.mu.Lock()
	delete(s.conn.activeStreams, s.id)
	s.conn.mu.Unlock()

	return nil
}
