package eventstream

import "testing"

// TestStreamOwner_RegisteredReturnsStream verifies that StreamOwner returns the
// stream registered at the given id (under c.mu).
func TestStreamOwner_RegisteredReturnsStream(t *testing.T) {
	c := &Connection{activeStreams: make(map[uint32]*Stream)}
	s := &Stream{id: 5, idAllocated: true, conn: c, operation: "test", done: make(chan struct{})}
	c.activeStreams[5] = s

	got, ok := c.StreamOwner(5)
	if !ok {
		t.Fatal("StreamOwner: expected ok=true for registered stream")
	}
	if got != s {
		t.Fatalf("StreamOwner: expected stream %p, got %p", s, got)
	}
}

// TestStreamOwner_UnknownIDReturnsFalse verifies that StreamOwner returns (nil, false)
// when no stream is registered at the given id.
func TestStreamOwner_UnknownIDReturnsFalse(t *testing.T) {
	c := &Connection{activeStreams: make(map[uint32]*Stream)}

	got, ok := c.StreamOwner(99)
	if ok {
		t.Fatal("StreamOwner: expected ok=false for unknown id")
	}
	if got != nil {
		t.Fatalf("StreamOwner: expected nil for unknown id, got %p", got)
	}
}

// TestStreamOwner_MismatchDetectable verifies the D1a scenario: id 7 is registered
// to liveStream; StreamOwner(7) returns liveStream, not resubStream, so the condition
// ok && owner != resubStream is true — the tripwire would fire.
func TestStreamOwner_MismatchDetectable(t *testing.T) {
	c := &Connection{activeStreams: make(map[uint32]*Stream)}
	liveStream := &Stream{id: 7, idAllocated: true, conn: c, operation: "live", done: make(chan struct{})}
	resubStream := &Stream{id: 7, idAllocated: true, conn: c, operation: "resub", done: make(chan struct{})}
	c.activeStreams[7] = liveStream

	owner, ok := c.StreamOwner(7)
	if !ok {
		t.Fatal("StreamOwner: expected ok=true when liveStream is registered at id 7")
	}
	// D1a condition: ok=true AND owner != resubStream → mismatch detected.
	if owner == resubStream {
		t.Fatal("invariant violated: registered owner is unexpectedly the resubStream")
	}
	if owner != liveStream {
		t.Fatalf("StreamOwner: expected liveStream, got %p", owner)
	}
}
