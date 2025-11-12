package eventstream

import (
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
)

// ConnectionError represents a socket-level connection failure
// These errors are retriable and should trigger reconnection
type ConnectionError struct {
	Err error
}

func (e *ConnectionError) Error() string {
	return fmt.Sprintf("connection error: %v", e.Err)
}

func (e *ConnectionError) Unwrap() error {
	return e.Err
}

// IsConnectionError checks if an error is a connection failure that should trigger reconnection
func IsConnectionError(err error) bool {
	if err == nil {
		return false
	}

	// Check if it's already wrapped as ConnectionError
	var connErr *ConnectionError
	if errors.As(err, &connErr) {
		return true
	}

	// Check for common network errors that indicate connection failure
	if errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.ETIMEDOUT) {
		return true
	}

	// Check for net.OpError (covers many socket errors)
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}

	return false
}

// WrapIfConnectionError wraps an error as ConnectionError if it's a network error
func WrapIfConnectionError(err error) error {
	if err == nil {
		return nil
	}
	if IsConnectionError(err) {
		return &ConnectionError{Err: err}
	}
	return err
}
