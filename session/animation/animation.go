package animation

import (
	"sync/atomic"

	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

// Animation is an interface used by sessions to manage visual animations during server transfers.
type Animation interface {
	// Play starts the animation on the given connection using the new server's GameData.
	Play(conn *minecraft.Conn, serverGameData minecraft.GameData)
	// Clear stops the animation previously started on the connection using the new server's GameData.
	Clear(conn *minecraft.Conn, serverGameData minecraft.GameData)
}

// PhasedAnimation separates the return to the target dimension from the final
// spawn notification. A ready-gated backend streams its authoritative player
// state and first chunk between these two calls.
type PhasedAnimation interface {
	Animation
	BeginClear(conn *minecraft.Conn, serverGameData minecraft.GameData)
	EndClear(conn *minecraft.Conn, serverGameData minecraft.GameData)
}

// AcknowledgedAnimation marks an animation whose first phase must finish on
// the client before Spectrum may start the target backend. Older Bedrock
// clients acknowledge ChangeDimension themselves and ignore a second
// ChangeDimension sent before that acknowledgement arrives.
type AcknowledgedAnimation interface {
	Animation
	RequiresAcknowledgement()
}

// NeedsAcknowledgement reports whether a transfer must wait for the client's
// first dimension-change acknowledgement before starting the target backend.
func NeedsAcknowledgement(a Animation) bool {
	_, ok := a.(AcknowledgedAnimation)
	return ok
}

// BeginClear starts the final half of an animation without exposing the
// client as spawned. Animations without a phased implementation remain active.
func BeginClear(a Animation, conn *minecraft.Conn, data minecraft.GameData) {
	if phased, ok := a.(PhasedAnimation); ok {
		phased.BeginClear(conn, data)
	}
}

// EndClear publishes the final spawn boundary. Legacy animations retain their
// original one-shot Clear implementation.
func EndClear(a Animation, conn *minecraft.Conn, data minecraft.GameData) {
	if phased, ok := a.(PhasedAnimation); ok {
		phased.EndClear(conn, data)
		return
	}
	a.Clear(conn, data)
}

// NopAnimation is a no-operation implementation of the Animation interface.
type NopAnimation struct{}

// Ensure that NopAnimation satisfies the Animation interface.
var _ Animation = NopAnimation{}

func (NopAnimation) Play(_ *minecraft.Conn, _ minecraft.GameData)  {}
func (NopAnimation) Clear(_ *minecraft.Conn, _ minecraft.GameData) {}

type cameraAnimation struct {
	synced atomic.Bool
}

func (animation *cameraAnimation) Sync(conn *minecraft.Conn) {
	if animation.synced.CompareAndSwap(false, true) {
		_ = conn.WritePacket(&packet.CameraPresets{
			Presets: []protocol.CameraPreset{
				{
					Name: "minecraft:free",
				},
			},
		})
	}
}
