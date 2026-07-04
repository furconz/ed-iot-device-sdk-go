package greengrassipc

import "testing"

func TestSubscription_LabelStored(t *testing.T) {
	s := &Subscription[IoTCoreMessage]{label: "nfc-command"}
	if s.label != "nfc-command" {
		t.Fatalf("label not stored: %q", s.label)
	}
}
