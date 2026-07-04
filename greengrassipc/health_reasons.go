package greengrassipc

import "github.com/furconz/ed-iot-device-sdk-go/internal/eventstream"

// Health-signal reason constants, re-exported from the internal eventstream package
// so external embedders can switch on them without importing internal packages.
//
// Use these in OnHealthSignal callbacks:
//
//	cfg.OnHealthSignal = func(reason string) {
//	    switch reason {
//	    case greengrassipc.ReasonRoutingMissOrphan, greengrassipc.ReasonStreamDrops:
//	        os.Exit(1)  // deafness — restart component
//	    case greengrassipc.ReasonReconnectFlap, greengrassipc.ReasonReconnectStuck:
//	        // log only — plain network outage, reconnect loop owns recovery
//	    }
//	}
const (
	ReasonRoutingMissOrphan = eventstream.ReasonRoutingMissOrphan
	ReasonStreamDrops       = eventstream.ReasonStreamDrops
	ReasonReconnectFlap     = eventstream.ReasonReconnectFlap
	ReasonReconnectStuck    = eventstream.ReasonReconnectStuck
)
