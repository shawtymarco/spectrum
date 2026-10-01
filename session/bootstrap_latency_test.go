package session

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/cooldogedev/spectrum/server"
)

type bootstrapTransport struct{ writes atomic.Int32 }

func (*bootstrapTransport) Read([]byte) (int, error)      { return 0, io.EOF }
func (c *bootstrapTransport) Write(p []byte) (int, error) { c.writes.Add(1); return len(p), nil }
func (*bootstrapTransport) Close() error                  { return nil }

func TestLatencyCannotPrecedeBackendConnectionRequest(t *testing.T) {
	transport := new(bootstrapTransport)
	s := &Session{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	s.serverConn = server.NewConn(transport, nil, s.logger, false, nil)
	// The public client deliberately remains nil: the bootstrap gate must not
	// even inspect it while a just-published backend is waiting for DoConnect.
	for range 20 {
		if err := s.writeLatency(); err != nil {
			t.Fatal(err)
		}
	}
	if transport.writes.Load() != 0 {
		t.Fatal("latency corrupted an uninitialised backend stream")
	}
}

func TestTransferDialFailurePreservesUnfrozenOrigin(t *testing.T) {
	want := errors.New("destination unavailable")
	origin := new(server.Conn)
	s := &Session{
		ctx:        context.Background(),
		serverConn: origin,
		serverAddr: "lobby:19142",
		processor:  NopProcessor{},
		transport:  testTransport{dial: func(context.Context, string) (io.ReadWriteCloser, error) { return nil, want }},
	}
	// No public client is installed: writing freeze metadata on a failed dial
	// would panic, and on a live client would leave the original scene immobile.
	if err := s.transferContext(context.Background(), "bedwars:19143", false); !errors.Is(err, want) {
		t.Fatalf("transfer error = %v", err)
	}
	if s.Server() != origin || s.serverAddr != "lobby:19142" {
		t.Fatal("failed dial retired the healthy origin")
	}
}
