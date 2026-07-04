package greengrassipc

import (
	"testing"

	"github.com/furconz/ed-iot-device-sdk-go/internal/eventstream"
)

// TestReasonConstantsMatchInternal asserts that the public greengrassipc.Reason* constants
// equal their internal eventstream counterparts.  This pins the re-export against silent
// drift if either side is renamed or its string value is changed.
func TestReasonConstantsMatchInternal(t *testing.T) {
	cases := []struct {
		name   string
		public string
		intern string
	}{
		{"RoutingMissOrphan", ReasonRoutingMissOrphan, eventstream.ReasonRoutingMissOrphan},
		{"StreamDrops", ReasonStreamDrops, eventstream.ReasonStreamDrops},
		{"ReconnectFlap", ReasonReconnectFlap, eventstream.ReasonReconnectFlap},
		{"ReconnectStuck", ReasonReconnectStuck, eventstream.ReasonReconnectStuck},
	}
	for _, tc := range cases {
		if tc.public != tc.intern {
			t.Errorf("Reason%s: public %q != internal %q", tc.name, tc.public, tc.intern)
		}
	}
}
