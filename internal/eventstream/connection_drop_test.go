package eventstream

import (
	"testing"
	"time"
)

// TestStreamDrop_ExitOnThreshold verifies that the 10th stream-level drop
// within the 30s window calls exitFunc(1) exactly once, while 9 drops do not.
func TestStreamDrop_ExitOnThreshold(t *testing.T) {
	origExit := exitFunc
	origNow := nowFunc
	t.Cleanup(func() {
		exitFunc = origExit
		nowFunc = origNow
	})

	exitCalls := 0
	exitCode := -1
	exitFunc = func(code int) {
		exitCalls++
		exitCode = code
	}

	// Pin time so all drops land in the same 30s window.
	fixedTime := time.Now()
	nowFunc = func() time.Time { return fixedTime }

	c := &Connection{}

	// 9 drops must not trigger exit.
	for i := 0; i < 9; i++ {
		c.recordStreamDrop()
	}
	if exitCalls != 0 {
		t.Fatalf("expected no exit after 9 drops within window, got %d exit call(s)", exitCalls)
	}

	// The 10th drop must trigger exitFunc(1) exactly once.
	c.recordStreamDrop()
	if exitCalls != 1 {
		t.Fatalf("expected exactly 1 exit call after 10th drop, got %d", exitCalls)
	}
	if exitCode != 1 {
		t.Fatalf("expected exit code 1, got %d", exitCode)
	}
}

// TestStreamDrop_WindowEviction verifies that drops spaced more than 30s apart
// never accumulate enough within the window to trigger exit.
func TestStreamDrop_WindowEviction(t *testing.T) {
	origExit := exitFunc
	origNow := nowFunc
	t.Cleanup(func() {
		exitFunc = origExit
		nowFunc = origNow
	})

	exitCalls := 0
	exitFunc = func(code int) { exitCalls++ }

	// Controllable clock: each drop is 31s after the previous.
	// At any given drop, the 30s window only contains that single drop,
	// so the counter never reaches dropThreshold (10).
	currentTime := time.Now()
	nowFunc = func() time.Time { return currentTime }

	c := &Connection{}

	for i := 0; i < 10; i++ {
		currentTime = currentTime.Add(31 * time.Second)
		c.recordStreamDrop()
	}

	if exitCalls != 0 {
		t.Fatalf("expected no exit when drops are spaced past the window, got %d exit call(s)", exitCalls)
	}
}
