package eventstream

import "testing"

func TestClose_StaleStreamDoesNotEvictLiveReusedID(t *testing.T) {
	c := &Connection{activeStreams: make(map[uint32]*Stream)}
	live := &Stream{id: 1, idAllocated: true, conn: c, operation: "live", done: make(chan struct{})}
	c.activeStreams[1] = live
	// Stale stream from a previous generation also holding id 1. closed=true so
	// Close() skips the TERMINATE writeMessage (no socket in this test).
	stale := &Stream{id: 1, idAllocated: true, closed: true, active: true, conn: c, operation: "stale", done: make(chan struct{})}
	if err := stale.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}
	if got, ok := c.activeStreams[1]; !ok || got != live {
		t.Fatalf("live stream at id 1 was evicted by stale Close; got=%v ok=%v", got, ok)
	}
}

func TestClose_OwnerStreamIsRemoved(t *testing.T) {
	c := &Connection{activeStreams: make(map[uint32]*Stream)}
	s := &Stream{id: 7, idAllocated: true, closed: true, active: true, conn: c, operation: "own", done: make(chan struct{})}
	c.activeStreams[7] = s
	if err := s.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}
	if _, ok := c.activeStreams[7]; ok {
		t.Fatalf("owner stream at id 7 was not removed")
	}
}
