package logging

import (
	"bytes"
	"log"
	"os"
	"strings"
	"testing"
)

func TestDebugGating(t *testing.T) {
	// Save original env var and restore at end
	originalValue := os.Getenv("IPC_DEBUG")
	defer func() {
		if originalValue == "" {
			os.Unsetenv("IPC_DEBUG")
		} else {
			os.Setenv("IPC_DEBUG", originalValue)
		}
		// Reset for other tests
		resetForTesting()
		checkDebugEnabled()
	}()

	tests := []struct {
		name          string
		envValue      string
		expectDebug   bool
		expectError   bool
	}{
		{
			name:        "Debug disabled by default",
			envValue:    "",
			expectDebug: false,
			expectError: true,
		},
		{
			name:        "Debug enabled with 'on'",
			envValue:    "on",
			expectDebug: true,
			expectError: true,
		},
		{
			name:        "Debug disabled with other value",
			envValue:    "off",
			expectDebug: false,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset package state
			resetForTesting()

			// Set environment variable
			if tt.envValue == "" {
				os.Unsetenv("IPC_DEBUG")
			} else {
				os.Setenv("IPC_DEBUG", tt.envValue)
			}

			// Capture log output
			var buf bytes.Buffer
			log.SetOutput(&buf)
			defer log.SetOutput(os.Stderr)
			log.SetFlags(0) // Remove timestamp for testing

			// Re-initialize the package
			checkDebugEnabled()

			// Test Debug() function
			Debug("test debug message")
			debugOutput := buf.String()

			if tt.expectDebug {
				if !strings.Contains(debugOutput, "[IPC DEBUG] test debug message") {
					t.Errorf("Expected debug output, got: %q", debugOutput)
				}
			} else {
				if strings.Contains(debugOutput, "test debug message") {
					t.Errorf("Expected no debug output, got: %q", debugOutput)
				}
			}

			// Test Error() function - should always output
			buf.Reset()
			Error("test error message")
			errorOutput := buf.String()

			if !strings.Contains(errorOutput, "[IPC ERROR] test error message") {
				t.Errorf("Expected error output, got: %q", errorOutput)
			}
		})
	}
}

func TestDebugPerformance(t *testing.T) {
	// Ensure debug is disabled
	resetForTesting()
	os.Unsetenv("IPC_DEBUG")
	checkDebugEnabled()

	// Capture output but don't test it - we're testing performance
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	// Call Debug many times - should be fast when disabled
	for i := 0; i < 10000; i++ {
		Debug("test message %d", i)
	}

	// Verify no output was generated
	if buf.Len() > 0 {
		t.Errorf("Expected no output when debug disabled, got %d bytes", buf.Len())
	}
}

func TestErrorAlwaysLogs(t *testing.T) {
	// Test with debug disabled
	resetForTesting()
	os.Unsetenv("IPC_DEBUG")
	checkDebugEnabled()

	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)
	log.SetFlags(0)

	Error("critical error: %s", "something failed")
	output := buf.String()

	if !strings.Contains(output, "[IPC ERROR] critical error: something failed") {
		t.Errorf("Expected error output even with debug disabled, got: %q", output)
	}
}
