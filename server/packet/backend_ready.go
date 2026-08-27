package packet

import "github.com/sandertv/gophertunnel/minecraft/protocol"

// BackendReady marks the point after which a transfer target emits only its
// final player state and final-world chunks. Spectrum drops target packets
// before this marker for transfers that requested a readiness barrier.
type BackendReady struct{}

// ID ...
func (*BackendReady) ID() uint32 { return IDBackendReady }

// Marshal ...
func (*BackendReady) Marshal(protocol.IO) {}
