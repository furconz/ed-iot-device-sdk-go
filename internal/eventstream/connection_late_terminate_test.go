package eventstream

// Regression tests for the non-monotonic stream-id protocol error (T1 RCA,
// docs/superpowers/research/2026-07-04-iot-stream-id-rca.md in ed-iot-rca).
//
// The fake nucleus below mirrors aws-c-event-stream <= v0.5.4 semantics
// (event_stream_rpc_server.c), which every deployed Greengrass nucleus embeds
// (2.17.0 -> aws-crt-java 0.33.5 -> aws-c-event-stream v0.5.0): when the server
// sends a response carrying TERMINATE_STREAM, its continuation is removed at
// flush; ANY later client frame for that stream-id — including a TERMINATE —
// hits the removed-continuation branch and produces the fatal connection-level
// "stream-id values must be monotonically incrementing" protocol error.
//
// The client must therefore never write a stream-id again once a
// terminate-flagged message has been received on that stream.

import (
	"context"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

// fakeNucleus accepts connections and models the pre-v0.5.5 server: any frame
// for a closed/unknown old stream-id is counted as a violation instead of
// hard-closing, so a single test run can count every occurrence.
type fakeNucleus struct {
	t          *testing.T
	violations atomic.Int64
}

func (f *fakeNucleus) serve(conn net.Conn) {
	defer conn.Close()
	var lastSeen uint32
	closedStreams := map[uint32]bool{} // continuation removed
	openStreams := map[uint32]bool{}

	for {
		msg, err := DecodeMessage(conn)
		if err != nil {
			return
		}

		switch msg.Type {
		case MessageTypeConnect:
			ack := CreateMessage(MessageTypeConnectAck, MessageFlagConnectionAccepted, nil)
			b, _ := EncodeMessage(ack)
			if _, err := conn.Write(b); err != nil {
				return
			}
			continue
		case MessageTypePing:
			pong := CreateMessage(MessageTypePingResponse, MessageFlagNone, nil)
			b, _ := EncodeMessage(pong)
			if _, err := conn.Write(b); err != nil {
				return
			}
			continue
		}

		var streamID uint32
		for _, h := range msg.Headers {
			if h.Name == ":stream-id" {
				if v, ok := h.Value.(int32); ok {
					streamID = uint32(v)
				}
			}
		}
		if streamID == 0 {
			continue
		}

		if streamID <= lastSeen {
			// "already seen stream_id, looking for existing continuation"
			if closedStreams[streamID] || !openStreams[streamID] {
				// pre-v0.5.5: fatal PROTOCOL_ERROR regardless of TERMINATE flag
				f.violations.Add(1)
				f.t.Logf("VIOLATION: frame for closed/unknown stream %d (flags=%v) after lastSeen=%d",
					streamID, msg.Flags, lastSeen)
				continue
			}
			// live continuation: a client TERMINATE closes it
			if msg.Flags.HasFlag(MessageFlagTerminateStream) {
				delete(openStreams, streamID)
				closedStreams[streamID] = true
			}
			continue
		}

		// New stream. The client allocates sequentially under lock, so ids on
		// the wire must be exactly lastSeen+1 (the server enforces this too).
		if streamID != lastSeen+1 {
			f.violations.Add(1)
			f.t.Logf("VIOLATION: new stream %d is not lastSeen+1 (lastSeen=%d)", streamID, lastSeen)
			continue
		}
		lastSeen = streamID
		openStreams[streamID] = true

		// Request-response: reply immediately with TERMINATE_STREAM and remove
		// the continuation on flush (event_stream_rpc_server.c:514-524 @ v0.5.4).
		resp := CreateMessageWithStreamID(MessageTypeApplicationMessage, MessageFlagTerminateStream, streamID, []byte(`{}`))
		b, _ := EncodeMessage(resp)
		if _, err := conn.Write(b); err != nil {
			return
		}
		delete(openStreams, streamID)
		closedStreams[streamID] = true
	}
}

// TestRequestResponse_NoFramesForTerminatedStreams stresses the deferred-Close
// vs readLoop race: pre-fix ~1 in 5000 request-responses echoed a TERMINATE
// for a continuation the server had already removed (observed field rate:
// twice in ~7h on an idle terminal). Post-fix the count must be zero.
func TestRequestResponse_NoFramesForTerminatedStreams(t *testing.T) {
	sockPath := filepath.Join(t.TempDir(), "ipc.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	nucleus := &fakeNucleus{t: t}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go nucleus.serve(conn)
		}
	}()

	c, err := Connect(context.Background(), ConnectionConfig{
		SocketPath:      sockPath,
		AuthToken:       "test",
		DisablePingPong: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	iterations := 30000
	if testing.Short() {
		iterations = 4000
	}
	const workers = 4 // concurrent requesters supply the scheduler pressure

	var wg sync.WaitGroup
	var reqErrs atomic.Int64
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations/workers; i++ {
				_, err := c.RequestResponse(context.Background(), "aws.greengrass#GetConfiguration", map[string]string{"k": "v"})
				if err != nil {
					reqErrs.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if n := reqErrs.Load(); n > 0 {
		t.Fatalf("%d request-response calls failed", n)
	}
	if v := nucleus.violations.Load(); v > 0 {
		t.Fatalf("client emitted %d frame(s) for already-terminated streams: on the deployed nucleus (aws-c-event-stream v0.5.0) each one is a fatal connection-level protocol error", v)
	}
}
