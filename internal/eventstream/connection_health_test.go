package eventstream

import (
	"testing"
	"time"
)

func TestHealth_GenuineOrphanSignals(t *testing.T) {
	t0 := time.Unix(1_700_000_000, 0)
	clock := t0
	nowFunc = func() time.Time { return clock }
	t.Cleanup(func() { nowFunc = time.Now })

	var got []string
	c := &Connection{
		activeStreams: make(map[uint32]*Stream),
		config:        ConnectionConfig{OnHealthSignal: func(r string) { got = append(got, r) }},
	}
	c.markConnected()                          // connectedAt = t0
	clock = t0.Add(startupGrace + time.Second) // advance past the startup grace
	for i := 0; i < orphanThreshold; i++ {
		c.recordRoutingMiss(uint32(1000 + i)) // genuine orphans, never cleanly-closed
	}
	if len(got) != 1 || got[0] != ReasonRoutingMissOrphan {
		t.Fatalf("expected one %q, got %v", ReasonRoutingMissOrphan, got)
	}
}

func TestHealth_QoS1DuplicateForRecentlyClosedIDIgnored(t *testing.T) {
	t0 := time.Unix(1_700_000_000, 0)
	clock := t0
	nowFunc = func() time.Time { return clock }
	t.Cleanup(func() { nowFunc = time.Now })

	var got []string
	c := &Connection{
		activeStreams: make(map[uint32]*Stream),
		config:        ConnectionConfig{OnHealthSignal: func(r string) { got = append(got, r) }},
	}
	c.markConnected()
	clock = t0.Add(startupGrace + time.Second)
	c.noteClosedID(42) // cleanly closed just now
	for i := 0; i < orphanThreshold*3; i++ {
		c.recordRoutingMiss(42) // QoS1 redelivery within closedIDTTL → ignored
	}
	if len(got) != 0 {
		t.Fatalf("recently-closed-id duplicate must not signal, got %v", got)
	}
}

func TestHealth_ReconnectFlapSignals(t *testing.T) {
	t0 := time.Unix(1_700_000_000, 0)
	clock := t0
	nowFunc = func() time.Time { return clock }
	t.Cleanup(func() { nowFunc = time.Now })

	var got []string
	c := &Connection{
		activeStreams: make(map[uint32]*Stream),
		config:        ConnectionConfig{OnHealthSignal: func(r string) { got = append(got, r) }},
	}
	// flapThreshold successful reconnects inside flapWindow → one flap signal.
	for i := 0; i < flapThreshold; i++ {
		clock = clock.Add(10 * time.Second) // well within flapWindow
		c.recordReconnectSuccess()
	}
	if len(got) != 1 || got[0] != ReasonReconnectFlap {
		t.Fatalf("expected one %q, got %v", ReasonReconnectFlap, got)
	}
}

// TestHealth_RecentlyClosedMapPrunedAfterTTL asserts two properties of the recentlyClosed
// prune logic:
//  1. After noteClosedID(id) and advancing the clock past closedIDTTL, a subsequent
//     noteClosedID call (for a different id) triggers the sweep and removes the expired entry.
//  2. After the prune, routing misses for that id count as genuine orphans (not suppressed).
func TestHealth_RecentlyClosedMapPrunedAfterTTL(t *testing.T) {
	t0 := time.Unix(1_700_000_000, 0)
	clock := t0
	nowFunc = func() time.Time { return clock }
	t.Cleanup(func() { nowFunc = time.Now })

	var got []string
	c := &Connection{
		activeStreams: make(map[uint32]*Stream),
		config:        ConnectionConfig{OnHealthSignal: func(r string) { got = append(got, r) }},
	}
	c.markConnected()
	clock = t0.Add(startupGrace + time.Second)

	// Record a clean close for id 42 at the current clock.
	c.noteClosedID(42)

	c.healthMu.Lock()
	if _, ok := c.recentlyClosed[42]; !ok {
		c.healthMu.Unlock()
		t.Fatal("id 42 should be in recentlyClosed immediately after noteClosedID")
	}
	c.healthMu.Unlock()

	// Advance clock well past the TTL window.
	clock = clock.Add(closedIDTTL + time.Second)

	// A subsequent noteClosedID for a different id triggers the sweep — 42 must be pruned.
	c.noteClosedID(99)

	c.healthMu.Lock()
	_, has42 := c.recentlyClosed[42]
	_, has99 := c.recentlyClosed[99]
	c.healthMu.Unlock()
	if has42 {
		t.Fatal("recentlyClosed still contains expired id 42 — prune not working")
	}
	if !has99 {
		t.Fatal("recentlyClosed should still contain recently-added id 99")
	}

	// Prove genuine-orphan: routing misses for the pruned id must count toward the threshold.
	for i := 0; i < orphanThreshold; i++ {
		c.recordRoutingMiss(42)
	}
	if len(got) != 1 || got[0] != ReasonRoutingMissOrphan {
		t.Fatalf("expected one %q after expired-id miss, got %v", ReasonRoutingMissOrphan, got)
	}
}

// TestHealth_StandaloneExitGating pins the fix for the nil-callback standalone fallback:
// self-heal reasons (routing-miss-orphan, stream-drops) MUST call exitFunc; connectivity
// reasons (reconnect-flap) must NOT call exitFunc (log-only — exiting on a plain network
// outage would crash-loop an offline device).
//
// ReasonReconnectStuck is not covered here because it is only reachable through a live
// dial in the reconnect loop (out-of-unit-scope); the same switch branch covers both
// connectivity reasons, so flap coverage is sufficient to pin the logic.
func TestHealth_StandaloneExitGating(t *testing.T) {
	origExit := exitFunc
	origNow := nowFunc
	t.Cleanup(func() {
		exitFunc = origExit
		nowFunc = origNow
	})

	// Replace exitFunc with a recorder so we can assert calls without killing the process.
	var exitCalls []int
	exitFunc = func(code int) { exitCalls = append(exitCalls, code) }

	t0 := time.Unix(1_700_000_000, 0)
	clock := t0
	nowFunc = func() time.Time { return clock }

	// --- connectivity reason: flap must NOT exit ---
	flapConn := &Connection{
		activeStreams: make(map[uint32]*Stream),
		config:        ConnectionConfig{}, // OnHealthSignal == nil → standalone fallback
	}
	for i := 0; i < flapThreshold; i++ {
		clock = clock.Add(10 * time.Second)
		flapConn.recordReconnectSuccess()
	}
	if len(exitCalls) != 0 {
		t.Fatalf("reconnect-flap must not call exitFunc in standalone mode, got %d call(s)", len(exitCalls))
	}

	// --- self-heal reason: orphan MUST exit ---
	orphanConn := &Connection{
		activeStreams: make(map[uint32]*Stream),
		config:        ConnectionConfig{}, // OnHealthSignal == nil → standalone fallback
	}
	orphanConn.markConnected()                          // connectedAt = current clock
	clock = clock.Add(startupGrace + time.Second)       // advance past startup grace
	for i := 0; i < orphanThreshold; i++ {
		orphanConn.recordRoutingMiss(uint32(2000 + i)) // genuine orphans
	}
	if len(exitCalls) != 1 || exitCalls[0] != 1 {
		t.Fatalf("routing-miss-orphan must call exitFunc(1) in standalone mode, got %v", exitCalls)
	}

	// --- self-heal reason: stream-drops MUST exit ---
	exitCalls = nil
	dropConn := &Connection{
		config: ConnectionConfig{}, // OnHealthSignal == nil → standalone fallback
	}
	// Pin clock so all drops land within the same dropWindow.
	nowFunc = func() time.Time { return clock }
	for i := 0; i < dropThreshold; i++ {
		dropConn.recordStreamDrop()
	}
	if len(exitCalls) != 1 || exitCalls[0] != 1 {
		t.Fatalf("stream-drops must call exitFunc(1) in standalone mode, got %v", exitCalls)
	}
}
