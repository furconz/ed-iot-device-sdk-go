// Package logging provides centralized logging for the Greengrass IPC SDK.
// Debug messages are gated behind the IPC_DEBUG environment variable for performance.
package logging

import (
	"log"
	"os"
	"sync"
)

var (
	debugEnabled bool
	once         sync.Once
	initialized  bool
)

// init checks the IPC_DEBUG environment variable once at package initialization.
func init() {
	checkDebugEnabled()
}

// checkDebugEnabled reads the IPC_DEBUG environment variable and caches the result.
func checkDebugEnabled() {
	once.Do(func() {
		debugEnabled = os.Getenv("IPC_DEBUG") == "on"
		initialized = true
	})
}

// resetForTesting resets the package state for testing purposes.
// This should only be used in tests.
func resetForTesting() {
	debugEnabled = false
	initialized = false
	once = sync.Once{}
}

// Debug logs debug messages only if IPC_DEBUG=on is set.
// Returns immediately with zero overhead if debug is disabled.
func Debug(format string, args ...interface{}) {
	if !debugEnabled {
		return
	}
	log.Printf("[IPC DEBUG] "+format, args...)
}

// Error logs error messages with the IPC ERROR prefix.
// These are always shown regardless of debug settings.
func Error(format string, args ...interface{}) {
	log.Printf("[IPC ERROR] "+format, args...)
}
