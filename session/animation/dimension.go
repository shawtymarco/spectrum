package animation

import (
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

// Dimension displays the dimension change screen to the player.
type Dimension struct{}

// AcknowledgedDimension performs the same dimension round-trip as Dimension,
// but leaves the acknowledgement to clients that predate the server-side
// DIMENSION_CHANGE_SUCCESS workaround.
type AcknowledgedDimension struct{}

// RequiresAcknowledgement marks the legacy two-phase transfer behaviour.
func (*AcknowledgedDimension) RequiresAcknowledgement() {}

// Play ...
func (animation *Dimension) Play(conn *minecraft.Conn, serverGameData minecraft.GameData) {
	sendDimension(conn, serverGameData, alternateDimension(serverGameData.Dimension), true, false)
}

// Clear ...
func (animation *Dimension) Clear(conn *minecraft.Conn, serverGameData minecraft.GameData) {
	_ = conn.WritePacket(&packet.PlayStatus{Status: packet.PlayStatusPlayerSpawn})
	sendDimension(conn, serverGameData, serverGameData.Dimension, true, true)
}

// BeginClear returns the client to the backend dimension but deliberately
// withholds PlayStatusPlayerSpawn until authoritative state and terrain arrive.
func (animation *Dimension) BeginClear(conn *minecraft.Conn, serverGameData minecraft.GameData) {
	sendDimension(conn, serverGameData, serverGameData.Dimension, true, false)
}

// EndClear releases the client after the target backend's first final chunk.
func (animation *Dimension) EndClear(conn *minecraft.Conn, _ minecraft.GameData) {
	_ = conn.WritePacket(&packet.PlayStatus{Status: packet.PlayStatusPlayerSpawn})
}

// Play starts the fake dimension phase and waits for the client's own
// PlayerActionDimensionChangeDone response.
func (*AcknowledgedDimension) Play(conn *minecraft.Conn, serverGameData minecraft.GameData) {
	sendDimension(conn, serverGameData, alternateDimension(serverGameData.Dimension), false, false)
}

// Clear completes both halves when a caller does not need phased control.
func (animation *AcknowledgedDimension) Clear(conn *minecraft.Conn, serverGameData minecraft.GameData) {
	animation.BeginClear(conn, serverGameData)
	animation.EndClear(conn, serverGameData)
}

// BeginClear returns to the backend dimension after the first client ACK.
func (*AcknowledgedDimension) BeginClear(conn *minecraft.Conn, serverGameData minecraft.GameData) {
	sendDimension(conn, serverGameData, serverGameData.Dimension, false, false)
}

// EndClear publishes the final spawn boundary after target streaming starts.
func (*AcknowledgedDimension) EndClear(conn *minecraft.Conn, _ minecraft.GameData) {
	_ = conn.WritePacket(&packet.PlayStatus{Status: packet.PlayStatusPlayerSpawn})
}

func alternateDimension(dimension int32) int32 {
	if dimension == packet.DimensionNether {
		return packet.DimensionEnd
	}
	return packet.DimensionNether
}

// sendDimension updates the player's dimension and optionally force-spawns them if playStatus is enabled.
func sendDimension(conn *minecraft.Conn, serverGameData minecraft.GameData, dimension int32, serverAcknowledgement, playStatus bool) {
	_ = conn.WritePacket(&packet.ChangeDimension{Dimension: dimension, Position: serverGameData.PlayerPosition})
	_ = conn.WritePacket(&packet.StopSound{StopAll: true})
	if serverAcknowledgement {
		_ = conn.WritePacket(&packet.PlayerAction{ActionType: protocol.PlayerActionDimensionChangeDone})
	}
	if playStatus {
		_ = conn.WritePacket(&packet.PlayStatus{
			Status: packet.PlayStatusPlayerSpawn,
		})
	}
}
