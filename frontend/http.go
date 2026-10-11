package frontend

import (
	"net"
	"sync"
)

// Limit sockets as well as active requests, so silent pre-header connections and
// keep-alive clients cannot create unbounded HTTP server goroutines.
type boundedHTTPListener struct {
	net.Listener
	slots chan struct{}
}

func (l *boundedHTTPListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		select {
		case l.slots <- struct{}{}:
			return &boundedHTTPConn{Conn: c, release: func() { <-l.slots }}, nil
		default:
			_ = c.Close()
		}
	}
}

type boundedHTTPConn struct {
	net.Conn
	once    sync.Once
	release func()
	err     error
}

func (c *boundedHTTPConn) Close() error {
	c.once.Do(func() { c.err = c.Conn.Close(); c.release() })
	return c.err
}

type httpWork struct {
	mu      sync.Mutex
	closing bool
	wg      sync.WaitGroup
}

func (w *httpWork) begin() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closing {
		return false
	}
	w.wg.Add(1)
	return true
}
func (w *httpWork) stop() { w.mu.Lock(); w.closing = true; w.mu.Unlock() }
