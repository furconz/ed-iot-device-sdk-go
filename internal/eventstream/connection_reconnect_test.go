package eventstream

// End-to-end reconnect churn test for the generation-safe lifecycle (T1 RCA
// defects 3/4): the fake nucleus hard-drops the connection every dropEvery
// requests; the client must reconnect and keep serving request-responses with
// zero stream-id violations on every connection generation. Pre-fix failure
// modes this pins down:
//   - defect 3: an Activate racing connect() pairs an old counter value with
//     the new socket → the fake nucleus flags "new stream N is not lastSeen+1"
//   - defect 4: a stale readLoop tears down the new generation's streams →
//     requests hang/fail persistently instead of recovering.

import (
	"context"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestReconnect_NoCrossGenerationViolations(t *testing.T) {
	if testing.Short() {
		t.Skip("reconnect churn test sleeps through real backoff")
	}

	sockPath := filepath.Join(t.TempDir(), "ipc.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	const dropEvery = 100 // hard-drop the connection after this many new streams

	nucleus := &fakeNucleus{t: t}
	var generations atomic.Int64
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			generations.Add(1)
			go nucleus.serveWithDrop(conn, dropEvery)
		}
	}()

	c, err := Connect(context.Background(), ConnectionConfig{
		SocketPath:         sockPath,
		AuthToken:          "test",
		DisablePingPong:    true,
		EnableReconnection: true,
		MaxRetries:         8,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	const workers = 3
	const perWorker = 120

	var wg sync.WaitGroup
	var successes, failures atomic.Int64
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				_, err := c.RequestResponse(ctx, "aws.greengrass#GetConfiguration", map[string]string{"k": "v"})
				cancel()
				if err != nil {
					failures.Add(1)
				} else {
					successes.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if v := nucleus.violations.Load(); v > 0 {
		t.Fatalf("%d stream-id violation(s) across reconnect generations", v)
	}
	if generations.Load() < 2 {
		t.Fatalf("expected at least one reconnect, saw %d connection(s)", generations.Load())
	}
	if successes.Load() == 0 {
		t.Fatal("no request-responses succeeded across reconnects")
	}
	// Requests may legitimately fail when they exhaust retries mid-drop, but the
	// connection must keep recovering: the vast majority must succeed.
	total := successes.Load() + failures.Load()
	if successes.Load() < total*9/10 {
		t.Fatalf("too many failures across reconnects: %d/%d succeeded", successes.Load(), total)
	}
	t.Logf("reconnect churn: %d generations, %d/%d requests succeeded, 0 violations",
		generations.Load(), successes.Load(), total)
}

// serveWithDrop behaves like serve but hard-closes the connection (mid-protocol,
// no goodbye) after dropEvery new streams, mimicking the nucleus tearing the
// connection down and forcing the client through its reconnect path.
func (f *fakeNucleus) serveWithDrop(conn net.Conn, dropEvery int) {
	defer conn.Close()
	var lastSeen uint32
	newStreams := 0
	closedStreams := map[uint32]bool{}
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
			if closedStreams[streamID] || !openStreams[streamID] {
				f.violations.Add(1)
				f.t.Logf("VIOLATION: frame for closed/unknown stream %d (flags=%v) after lastSeen=%d",
					streamID, msg.Flags, lastSeen)
				continue
			}
			if msg.Flags.HasFlag(MessageFlagTerminateStream) {
				delete(openStreams, streamID)
				closedStreams[streamID] = true
			}
			continue
		}

		if streamID != lastSeen+1 {
			f.violations.Add(1)
			f.t.Logf("VIOLATION: new stream %d is not lastSeen+1 (lastSeen=%d)", streamID, lastSeen)
			continue
		}
		lastSeen = streamID
		openStreams[streamID] = true
		newStreams++

		resp := CreateMessageWithStreamID(MessageTypeApplicationMessage, MessageFlagTerminateStream, streamID, []byte(`{}`))
		b, _ := EncodeMessage(resp)
		if _, err := conn.Write(b); err != nil {
			return
		}
		delete(openStreams, streamID)
		closedStreams[streamID] = true

		if newStreams >= dropEvery {
			return // deferred conn.Close(): hard drop, client must reconnect
		}
	}
}
