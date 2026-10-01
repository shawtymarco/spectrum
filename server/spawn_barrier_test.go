package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"

	spectrumprotocol "github.com/cooldogedev/spectrum/protocol"
)

func TestSpawnBarrierWaitsForInitialisationWrite(t *testing.T) {
	writer, reader := net.Pipe()
	defer writer.Close()
	defer reader.Close()
	transport := &spawnBarrierTransport{Conn: writer, entered: make(chan struct{})}
	c := NewConn(transport, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), false, nil)
	if c.Spawned() {
		t.Fatal("new backend accepts input before bootstrap")
	}
	done := make(chan error, 1)
	go func() { done <- c.DoSpawn() }()
	<-transport.entered
	if c.Spawned() {
		t.Fatal("blocked spawn request opened the client-input gate")
	}
	if _, err := spectrumprotocol.NewReader(reader).ReadPacket(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !c.Spawned() {
		t.Fatal("completed spawn request did not release input")
	}
	c.CloseWithError(context.Canceled)
	if c.Spawned() {
		t.Fatal("retired backend still accepts input")
	}
}

type spawnBarrierTransport struct {
	net.Conn
	once    sync.Once
	entered chan struct{}
}

func (c *spawnBarrierTransport) Write(p []byte) (int, error) {
	c.once.Do(func() { close(c.entered) })
	return c.Conn.Write(p)
}

type failedSpawnTransport struct{ lifecycleTransport }

func (*failedSpawnTransport) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestFailedSpawnNeverOpensInputGate(t *testing.T) {
	c := NewConn(new(failedSpawnTransport), nil, slog.New(slog.NewTextHandler(io.Discard, nil)), false, nil)
	if err := c.DoSpawn(); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("spawn error = %v", err)
	}
	if c.Spawned() {
		t.Fatal("failed spawn released client input")
	}
}
