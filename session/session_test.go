package session

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/cooldogedev/spectrum/server"
	"github.com/cooldogedev/spectrum/util"
	"github.com/sandertv/gophertunnel/minecraft"
)

type testTransport struct {
	dial func(context.Context, string) (io.ReadWriteCloser, error)
}

type failingDiscovery struct {
	err error
}

func (f failingDiscovery) Discover(*minecraft.Conn) (string, error) { return "", f.err }
func (f failingDiscovery) DiscoverFallback(*minecraft.Conn) (string, error) {
	return "", f.err
}

func (t testTransport) Dial(ctx context.Context, addr string) (io.ReadWriteCloser, error) {
	return t.dial(ctx, addr)
}

type testConn struct {
	onClose func()
}

func (*testConn) Read([]byte) (int, error)    { return 0, io.EOF }
func (*testConn) Write(p []byte) (int, error) { return len(p), nil }
func (c *testConn) Close() error {
	if c.onClose != nil {
		c.onClose()
	}
	return nil
}

func TestDialPublishesReplacementBeforeClosingPrevious(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	replacementTransport := &testConn{}
	s := &Session{
		logger: logger,
		opts:   *util.DefaultOpts(),
		transport: testTransport{dial: func(_ context.Context, addr string) (io.ReadWriteCloser, error) {
			if addr != "bedwars:19143" {
				t.Fatalf("unexpected address %q", addr)
			}
			return replacementTransport, nil
		}},
	}
	s.ctx, s.cancelFunc = context.WithCancelCause(context.Background())
	s.cache.Store([]byte(nil))

	previousTransport := &testConn{}
	previous := server.NewConn(previousTransport, nil, logger, false, nil)
	s.serverAddr = "lobby:19142"
	s.serverConn = previous
	previousTransport.onClose = func() {
		if got := s.Server(); got == previous {
			t.Error("previous backend was closed before the replacement was published")
		}
	}

	replacement, err := s.dial(context.Background(), "bedwars:19143")
	if err != nil {
		t.Fatalf("dial replacement: %v", err)
	}
	if got := s.Server(); got != replacement {
		t.Fatal("replacement backend was not published")
	}
	if s.serverAddr != "bedwars:19143" {
		t.Fatalf("server address = %q, want bedwars:19143", s.serverAddr)
	}
}

func TestDialRejectsEmptyAddress(t *testing.T) {
	dialed := false
	s := &Session{
		transport: testTransport{dial: func(context.Context, string) (io.ReadWriteCloser, error) {
			dialed = true
			return nil, errors.New("unexpected dial")
		}},
	}
	s.ctx, s.cancelFunc = context.WithCancelCause(context.Background())

	if _, err := s.dial(context.Background(), "  "); err == nil || err.Error() != "server address is empty" {
		t.Fatalf("dial error = %v, want server address is empty", err)
	}
	if dialed {
		t.Fatal("transport dialed an empty address")
	}
}

func TestBackendIsCurrentRejectsRetiredConnection(t *testing.T) {
	retired := new(server.Conn)
	current := new(server.Conn)
	s := &Session{serverConn: current, serverAddr: "bedwars:19143"}

	if ok, addr := s.backendIsCurrent(current); !ok || addr != "bedwars:19143" {
		t.Fatalf("current backend = (%v, %q), want (true, bedwars:19143)", ok, addr)
	}
	if ok, addr := s.backendIsCurrent(retired); ok || addr != "bedwars:19143" {
		t.Fatalf("retired backend = (%v, %q), want (false, bedwars:19143)", ok, addr)
	}
}

func TestFallbackReleasesGuardAfterSynchronousFailure(t *testing.T) {
	expected := errors.New("discovery unavailable")
	s := &Session{discovery: failingDiscovery{err: expected}, processor: NopProcessor{}}
	s.ctx, s.cancelFunc = context.WithCancelCause(context.Background())

	for attempt := 0; attempt < 2; attempt++ {
		err := s.fallback()
		if !errors.Is(err, expected) {
			t.Fatalf("fallback attempt %d error = %v, want %v", attempt+1, err, expected)
		}
		if s.inFallback.Load() {
			t.Fatalf("fallback attempt %d retained the in-progress guard", attempt+1)
		}
	}
}
