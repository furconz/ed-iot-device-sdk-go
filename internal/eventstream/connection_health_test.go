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
