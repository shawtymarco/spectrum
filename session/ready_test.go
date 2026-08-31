package session

import (
	"io"
	"log/slog"
	"testing"

	"github.com/cooldogedev/spectrum/server"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

type readyAnimation struct{ began bool }

func (*readyAnimation) Play(*minecraft.Conn, minecraft.GameData)  {}
func (*readyAnimation) Clear(*minecraft.Conn, minecraft.GameData) {}
func (a *readyAnimation) BeginClear(*minecraft.Conn, minecraft.GameData) {
	a.began = true
}

func TestDimensionAcknowledgementIdentification(t *testing.T) {
	if !isDimensionAcknowledgement(&packet.PlayerAction{ActionType: protocol.PlayerActionDimensionChangeDone}) {
		t.Fatal("dimension acknowledgement was not identified")
	}
	if isDimensionAcknowledgement(&packet.PlayerAction{ActionType: protocol.PlayerActionStartBreak}) {
		t.Fatal("ordinary player action was identified as a dimension acknowledgement")
	}
	if isDimensionAcknowledgement(&packet.PlayStatus{Status: packet.PlayStatusPlayerSpawn}) {
		t.Fatal("non-player-action packet was identified as a dimension acknowledgement")
	}
}

func TestAcknowledgementGateIsBackendScoped(t *testing.T) {
	backend := new(server.Conn)
	other := new(server.Conn)
	s := &Session{acknowledged: &acknowledgedTransfer{backend: backend}}
	if !s.acknowledgementWaiting(backend) {
		t.Fatal("acknowledgement gate did not match its backend")
	}
	if s.acknowledgementWaiting(other) {
		t.Fatal("acknowledgement gate matched another backend")
	}
}
func (*readyAnimation) EndClear(*minecraft.Conn, minecraft.GameData) {}

func TestBackendReadyStartsAuthoritativeStreaming(t *testing.T) {
	backend := new(server.Conn)
	animation := &readyAnimation{}
	s := &Session{
		animation: animation,
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		ready: &readyTransfer{
			backend: backend,
			target:  "replay:19144",
			phase:   readyTransferWaiting,
		},
	}
	if !s.backendWaiting(backend) {
		t.Fatal("pending backend was not gated")
	}
	if err := s.markBackendReady(backend); err != nil {
		t.Fatal(err)
	}
	if s.backendWaiting(backend) {
		t.Fatal("ready backend remained gated")
	}
	if !animation.began {
		t.Fatal("ready marker did not begin the final animation phase")
	}
	if err := s.markBackendReady(backend); err == nil {
		t.Fatal("duplicate ready marker was accepted")
	}
}

func TestReadyTimeoutCoversStreamingPhase(t *testing.T) {
	backend := new(server.Conn)
	s := &Session{ready: &readyTransfer{backend: backend, phase: readyTransferStreaming}}
	if !s.expireReadyTransfer(backend) {
		t.Fatal("streaming transfer was not expired")
	}
	if s.ready != nil {
		t.Fatal("expired transfer remained installed")
	}
}
