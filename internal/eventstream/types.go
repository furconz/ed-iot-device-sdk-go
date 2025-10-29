// Package eventstream implements the AWS EventStream RPC protocol
// used for Greengrass IPC communication over Unix domain sockets.
package eventstream

import (
	"encoding/json"
	"fmt"
)

// MessageType represents the type of EventStream message
type MessageType uint32

const (
	// MessageTypeConnect is sent by client to initiate connection
	MessageTypeConnect MessageType = 0

	// MessageTypeConnectAck is sent by server in response to Connect
	MessageTypeConnectAck MessageType = 1

	// MessageTypeApplicationMessage is used for operation requests/responses
	MessageTypeApplicationMessage MessageType = 3

	// MessageTypeApplicationError is used for operation errors
	MessageTypeApplicationError MessageType = 4
)

// String returns string representation of MessageType
func (m MessageType) String() string {
	switch m {
	case MessageTypeConnect:
		return "Connect"
	case MessageTypeConnectAck:
		return "ConnectAck"
	case MessageTypeApplicationMessage:
		return "ApplicationMessage"
	case MessageTypeApplicationError:
		return "ApplicationError"
	default:
		return fmt.Sprintf("Unknown(%d)", m)
	}
}

// MessageFlags represents bitwise flags for EventStream messages
type MessageFlags uint32

const (
	// MessageFlagNone indicates no flags set
	MessageFlagNone MessageFlags = 0

	// MessageFlagConnectionAccepted indicates connection was accepted (used in ConnectAck)
	MessageFlagConnectionAccepted MessageFlags = 1 << 0

	// MessageFlagTerminateStream indicates the stream should be closed
	MessageFlagTerminateStream MessageFlags = 1 << 1
)

// HasFlag checks if a specific flag is set
func (f MessageFlags) HasFlag(flag MessageFlags) bool {
	return (f & flag) != 0
}

// String returns string representation of MessageFlags
func (f MessageFlags) String() string {
	if f == MessageFlagNone {
		return "None"
	}
	var flags []string
	if f.HasFlag(MessageFlagConnectionAccepted) {
		flags = append(flags, "ConnectionAccepted")
	}
	if f.HasFlag(MessageFlagTerminateStream) {
		flags = append(flags, "TerminateStream")
	}
	return fmt.Sprintf("%v", flags)
}

// HeaderType represents the type of a header value
type HeaderType byte

const (
	// HeaderTypeBoolTrue represents a boolean true value
	HeaderTypeBoolTrue HeaderType = 0

	// HeaderTypeBoolFalse represents a boolean false value
	HeaderTypeBoolFalse HeaderType = 1

	// HeaderTypeByte represents a single byte value
	HeaderTypeByte HeaderType = 2

	// HeaderTypeInt16 represents a 16-bit integer
	HeaderTypeInt16 HeaderType = 3

	// HeaderTypeInt32 represents a 32-bit integer
	HeaderTypeInt32 HeaderType = 4

	// HeaderTypeInt64 represents a 64-bit integer
	HeaderTypeInt64 HeaderType = 5

	// HeaderTypeByteArray represents a byte array
	HeaderTypeByteArray HeaderType = 6

	// HeaderTypeString represents a UTF-8 string
	HeaderTypeString HeaderType = 7

	// HeaderTypeTimestamp represents a timestamp (milliseconds since epoch)
	HeaderTypeTimestamp HeaderType = 8

	// HeaderTypeUUID represents a UUID
	HeaderTypeUUID HeaderType = 9
)

// Header represents a single header in an EventStream message
type Header struct {
	Name  string
	Type  HeaderType
	Value interface{}
}

// Message represents a complete EventStream message
type Message struct {
	Type    MessageType
	Flags   MessageFlags
	Headers []Header
	Payload []byte
}

// GetHeader retrieves a header value by name
func (m *Message) GetHeader(name string) (interface{}, bool) {
	for _, h := range m.Headers {
		if h.Name == name {
			return h.Value, true
		}
	}
	return nil, false
}

// GetStringHeader retrieves a string header value
func (m *Message) GetStringHeader(name string) (string, bool) {
	val, ok := m.GetHeader(name)
	if !ok {
		return "", false
	}
	str, ok := val.(string)
	return str, ok
}

// SetHeader sets or updates a header value
func (m *Message) SetHeader(name string, headerType HeaderType, value interface{}) {
	// Check if header already exists and update it
	for i, h := range m.Headers {
		if h.Name == name {
			m.Headers[i].Type = headerType
			m.Headers[i].Value = value
			return
		}
	}
	// Add new header
	m.Headers = append(m.Headers, Header{
		Name:  name,
		Type:  headerType,
		Value: value,
	})
}

// OperationError represents an error returned from an operation
type OperationError struct {
	ErrorType        string          // Service model type of the error
	ErrorMessage     string          // Human-readable error message
	ModeledError     json.RawMessage // The full modeled error structure
	IsServiceError   bool            // True if this is a modeled service error
	IsInternalError  bool            // True if this is an internal error
	UnderlyingError  error           // Underlying error if any
}

// Error implements the error interface
func (e *OperationError) Error() string {
	if e.ErrorMessage != "" {
		return fmt.Sprintf("%s: %s", e.ErrorType, e.ErrorMessage)
	}
	return e.ErrorType
}

// Unwrap returns the underlying error
func (e *OperationError) Unwrap() error {
	return e.UnderlyingError
}

// ConnectRequest is the payload for CONNECT messages
type ConnectRequest struct {
	AuthToken string `json:"authToken"`
}

// ConnectResponse is the payload for CONNECTACK messages
type ConnectResponse struct {
	// Currently empty, but defined for future extensibility
}
