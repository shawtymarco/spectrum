package session

import (
	"io"
	"log/slog"
	"testing"

	"github.com/cooldogedev/spectrum/server"
	"github.com/sandertv/gophertunnel/minecraft"
)

type readyAnimation struct{ began bool }

func (*readyAnimation) Play(*minecraft.Conn, minecraft.GameData)  {}
func (*readyAnimation) Clear(*minecraft.Conn, minecraft.GameData) {}
func (a *readyAnimation) BeginClear(*minecraft.Conn, minecraft.GameData) {
	a.began = true
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
