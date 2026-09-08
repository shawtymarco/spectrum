package server

import (
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

type lifecycleTransport struct{ closes atomic.Int32 }

func (*lifecycleTransport) Read([]byte) (int, error)    { return 0, io.EOF }
func (*lifecycleTransport) Write(p []byte) (int, error) { return len(p), nil }
func (t *lifecycleTransport) Close() error              { t.closes.Add(1); return nil }

func lifecycleConn() (*Conn, *lifecycleTransport) {
	transport := new(lifecycleTransport)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewConn(transport, nil, logger, false, nil), transport
}

func TestConnectRegistrationRacesClose(t *testing.T) {
	wantErr := errors.New("backend closed before callback registration")
	for i := 0; i < 1000; i++ {
		conn, transport := lifecycleConn()
		var calls atomic.Int32
		var wrongOutcome atomic.Bool
		start := make(chan struct{})
		var workers sync.WaitGroup
		workers.Add(2)
		go func() {
			defer workers.Done()
			<-start
			conn.OnConnect(func(err error) {
				calls.Add(1)
				if !errors.Is(err, wantErr) {
					wrongOutcome.Store(true)
				}
			})
		}()
		go func() { defer workers.Done(); <-start; conn.CloseWithError(wantErr) }()
		close(start)
		workers.Wait()
		if calls.Load() != 1 || wrongOutcome.Load() || transport.closes.Load() != 1 {
			t.Fatalf("callback lost/duplicated: calls=%d wrong=%v closes=%d", calls.Load(), wrongOutcome.Load(), transport.closes.Load())
		}
	}
}

func TestConnectSuccessAndCloseDeliverOneOutcome(t *testing.T) {
	for i := 0; i < 1000; i++ {
		conn, _ := lifecycleConn()
		var calls atomic.Int32
		conn.OnConnect(func(error) { calls.Add(1) })
		var workers sync.WaitGroup
		workers.Add(2)
		go func() { defer workers.Done(); _ = conn.handlePlayStatus(&packet.PlayStatus{}) }()
		go func() { defer workers.Done(); _ = conn.Close() }()
		workers.Wait()
		if calls.Load() != 1 {
			t.Fatalf("handshake and close delivered %d outcomes", calls.Load())
		}
	}
}

func TestConnectCallbackMayReenterClose(t *testing.T) {
	for _, success := range []bool{false, true} {
		conn, transport := lifecycleConn()
		done := make(chan struct{})
		conn.OnConnect(func(error) { _ = conn.Close(); close(done) })
		go func() {
			if success {
				_ = conn.handlePlayStatus(&packet.PlayStatus{})
			} else {
				_ = conn.Close()
			}
		}()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("callback deadlocked against connection close")
		}
		if transport.closes.Load() != 1 {
			t.Fatal("reentrant callback closed the underlying transport twice")
		}
	}
}

func TestConnectLateRegistrationAndRepeatedSuccess(t *testing.T) {
	conn, _ := lifecycleConn()
	_ = conn.handlePlayStatus(&packet.PlayStatus{})
	_ = conn.handlePlayStatus(&packet.PlayStatus{})
	calls := 0
	conn.OnConnect(func(err error) {
		calls++
		if err != nil {
			t.Fatal("successful handshake outcome changed")
		}
	})
	_ = conn.Close()
	if calls != 1 {
		t.Fatalf("late registration got %d callbacks", calls)
	}
}
