package greengrassipc

import (
	"sync"
	"testing"
)

func TestResubscribe_OncePerGeneration(t *testing.T) {
	var calls int
	var mu sync.Mutex
	do := func() bool { mu.Lock(); calls++; mu.Unlock(); return true }
	g := &generationGuard{}
	gen := uint64(5)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); g.once(gen, do) }()
	}
	wg.Wait()
	if calls != 1 {
		t.Fatalf("expected exactly 1 resubscribe for generation %d, got %d", gen, calls)
	}
	g.once(gen+1, do)
	if calls != 2 {
		t.Fatalf("expected resubscribe on new generation, got %d", calls)
	}
}
