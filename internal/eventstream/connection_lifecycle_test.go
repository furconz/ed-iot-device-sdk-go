package eventstream

// Tests for the generation-safe connection lifecycle (T1 RCA defects 3 and 4):
// Activate must never run against a down/mid-reconnect connection, and only
// the readLoop of the CURRENT generation may tear down shared connection state.

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestActivate_FailsFastWhenDisconnected(t *testing.T) {
	c := &Connection{connected: false, nextStreamID: 1, activeStreams: make(map[uint32]*Stream)}
	s := c.NewStream("op")
	err := s.Activate(context.Background(), map[string]string{"k": "v"})
	if err == nil || !IsConnectionError(err) {
		t.Fatalf("want retriable ConnectionError, got %v", err)
	}
	if s.idAllocated || len(c.activeStreams) != 0 || c.nextStreamID != 1 {
		t.Fatalf("no stream id may be allocated while disconnected (idAllocated=%v, streams=%d, next=%d)",
			s.idAllocated, len(c.activeStreams), c.nextStreamID)
	}
}

// eofConn returns a net.Conn whose reads fail immediately with EOF.
func eofConn(t *testing.T) net.Conn {
	t.Helper()
	r, w := net.Pipe()
	w.Close()
	return r
}

func lifecycleConn(gen uint64) (*Connection, *Stream) {
	stream := &Stream{
		id:          1,
		idAllocated: true,
		active:      true,
		messages:    make(chan *Message, 1),
		errors:      make(chan error, 1),
		done:        make(chan struct{}),
	}
	c := &Connection{
		connected:     true,
		gen:           gen,
		activeStreams: map[uint32]*Stream{1: stream},
		incoming:      make(chan *Message, 1),
	}
	stream.conn = c
	return c, stream
}

func TestReadLoop_StaleGenerationExitsWithoutTeardown(t *testing.T) {
	c, stream := lifecycleConn(2)
	c.readLoop(eofConn(t), 1) // stale: connection has moved to gen 2

	if !c.connected {
		t.Fatal("stale readLoop must not mark the successor connection disconnected")
	}
	select {
	case <-stream.done:
		t.Fatal("stale readLoop must not close the successor generation's streams")
	default:
	}
	select {
	case err := <-stream.errors:
		t.Fatalf("stale readLoop must not error successor streams, got %v", err)
	default:
	}
	// incoming must still be open and usable
	select {
	case c.incoming <- &Message{}:
	default:
		t.Fatal("incoming channel unusable after stale readLoop exit")
	}
}

func TestReadLoop_CurrentGenerationTearsDown(t *testing.T) {
	c, stream := lifecycleConn(1)
	c.readLoop(eofConn(t), 1) // matching generation owns teardown

	if c.connected {
		t.Fatal("current-generation readLoop must mark the connection disconnected")
	}
	select {
	case <-stream.done:
	default:
		t.Fatal("current-generation readLoop must close its streams")
	}
	if err := <-stream.errors; !IsConnectionError(err) {
		t.Fatalf("stream must receive a retriable ConnectionError, got %v", err)
	}
	if _, ok := <-c.incoming; ok {
		t.Fatal("incoming channel must be closed by the owning readLoop")
	}
}

// TestReadLoop_MarksPeerTerminatedBeforeDelivery pins the T1 ordering fix:
// by the time a terminate-flagged message is received by a consumer, the
// stream must already be marked peerTerminated, so an immediate Close cannot
// echo a TERMINATE for the server-removed continuation.
func TestReadLoop_MarksPeerTerminatedBeforeDelivery(t *testing.T) {
	c, stream := lifecycleConn(1)
	r, w := net.Pipe()
	go c.readLoop(r, 1)

	msg := CreateMessageWithStreamID(MessageTypeApplicationMessage, MessageFlagTerminateStream, 1, []byte(`{}`))
	encoded, err := EncodeMessage(msg)
	if err != nil {
		t.Fatal(err)
	}
	go w.Write(encoded) //nolint:errcheck // net.Pipe write completes only when read

	select {
	case <-stream.messages:
	case <-time.After(5 * time.Second):
		t.Fatal("terminate-flagged message was not delivered")
	}

	stream.mu.Lock()
	pt := stream.peerTerminated
	stream.mu.Unlock()
	if !pt {
		t.Fatal("peerTerminated must be set BEFORE the terminating message is delivered")
	}

	// An immediate consumer Close must not write anything: c.conn is nil here,
	// so an erroneous TERMINATE would panic inside writeMessage.
	if err := stream.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}
	select {
	case <-stream.done:
	default:
		t.Fatal("Close must complete the done-channel lifecycle when skipping TERMINATE")
	}

	w.Close() // let the readLoop exit via its (gen-matched) error path
}
