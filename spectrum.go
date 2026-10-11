package spectrum

import (
	"context"
	"errors"
	"fmt"
	"github.com/cooldogedev/spectrum/frontend"
	"log/slog"
	"net"
	"slices"
	"sync"

	"github.com/cooldogedev/spectrum/server"
	"github.com/cooldogedev/spectrum/session"
	tr "github.com/cooldogedev/spectrum/transport"
	"github.com/cooldogedev/spectrum/util"
	"github.com/sandertv/gophertunnel/minecraft"
)

// Spectrum represents a proxy server managing server discovery,
// network transport, and connections through an underlying minecraft.Listener.
type Spectrum struct {
	discovery server.Discovery
	transport tr.Transport

	mu        sync.Mutex
	listeners []*minecraft.Listener
	incoming  chan *minecraft.Conn
	done      chan struct{}
	closed    chan struct{}
	closing   bool
	acceptWG  sync.WaitGroup
	loginWG   sync.WaitGroup
	sessionWG sync.WaitGroup
	sessions  map[*session.Session]struct{}
	closeErr  error
	registry  *session.Registry

	logger *slog.Logger
	opts   util.Opts
}

// NewSpectrum creates a new Spectrum instance using the provided server.Discovery.
// It initializes opts with default options from util.DefaultOpts() if opts is nil,
// and defaults to TCP transport if transport is nil transport.TCP.
func NewSpectrum(discovery server.Discovery, logger *slog.Logger, opts *util.Opts, transport tr.Transport) *Spectrum {
	if opts == nil {
		opts = util.DefaultOpts()
	}

	if logger == nil {
		logger = slog.Default()
	}
	if transport == nil {
		transport = tr.NewSpectral(logger)
	}
	return &Spectrum{
		discovery: discovery,
		transport: transport,

		registry: session.NewRegistry(),
		incoming: make(chan *minecraft.Conn),
		done:     make(chan struct{}),
		closed:   make(chan struct{}),
		sessions: make(map[*session.Session]struct{}),

		logger: logger,
		opts:   *opts,
	}
}

// Listen sets up a minecraft.Listener for incoming connections based on the provided minecraft.ListenConfig.
// The listener is then used by the Accept() method for accepting incoming connections.
func (s *Spectrum) Listen(config minecraft.ListenConfig) (err error) {
	return s.ListenWith(config, func(cfg minecraft.ListenConfig) (*minecraft.Listener, error) {
		return cfg.Listen("raknet", s.opts.Addr)
	})
}

// ListenWith binds all public transports as one logical endpoint. Factories receive
// the same policy and ListenerGroup. A factory owns rollback on its own failure;
// Spectrum owns all listeners returned successfully, including partial startup.
// Factories must preserve the shared policy, clearing MaximumMTU for non-RakNet
// transports. Calls are serialized with Close. A failed bind may be retried.
func (s *Spectrum) ListenWith(config minecraft.ListenConfig, bind ...func(minecraft.ListenConfig) (*minecraft.Listener, error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return net.ErrClosed
	}
	if len(s.listeners) != 0 {
		return errors.New("Spectrum is already listening")
	}
	if len(bind) == 0 {
		return errors.New("no public listeners configured")
	}
	if config.ListenerGroup == nil {
		config.ListenerGroup = new(minecraft.ListenerGroup)
	}
	var listeners []*minecraft.Listener
	for i, factory := range bind {
		var listener *minecraft.Listener
		var err error
		if factory == nil {
			err = errors.New("nil listener factory")
		} else {
			listener, err = factory(config)
		}
		if listener != nil {
			listeners = append(listeners, listener)
		}
		if err != nil || listener == nil {
			if err == nil {
				err = errors.New("factory returned nil listener")
			}
			for _, l := range listeners {
				err = errors.Join(err, l.Close())
			}
			return fmt.Errorf("public listener %d: %w", i, err)
		}
	}
	s.listeners = listeners
	for _, listener := range listeners {
		s.acceptWG.Add(1)
		go s.accept(listener)
		s.logger.Info("started listening", "addr", listener.Addr())
	}
	return nil
}

func (s *Spectrum) accept(listener *minecraft.Listener) {
	defer s.acceptWG.Done()
	for {
		c, err := listener.Accept()
		if err != nil {
			// A required listener dying closes the entire logical endpoint.
			go s.Close()
			return
		}
		conn := c.(*minecraft.Conn)
		frontend.CompleteBootstrap(conn)
		select {
		case s.incoming <- conn:
		case <-s.done:
			_ = conn.Close()
			return
		}
	}
}

// Accept accepts an incoming minecraft.Conn and creates a new session for it.
// This method should be called in a loop to continuously accept new connections.
func (s *Spectrum) Accept() (*session.Session, error) {
	var conn *minecraft.Conn
	select {
	case conn = <-s.incoming:
	case <-s.done:
		return nil, net.ErrClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		_ = conn.Close()
		return nil, net.ErrClosed
	}
	identityData := conn.IdentityData()
	logger := s.logger.With("username", identityData.DisplayName)
	newSession := session.NewSession(conn, logger, s.registry, s.discovery, s.opts, s.transport)
	s.sessions[newSession] = struct{}{}
	s.sessionWG.Add(1)
	go func() {
		defer s.sessionWG.Done()
		<-newSession.Context().Done()
		_ = newSession.Close()
		s.mu.Lock()
		delete(s.sessions, newSession)
		s.mu.Unlock()
	}()
	if s.opts.AutoLogin {
		s.loginWG.Add(1)
		go func() {
			defer s.loginWG.Done()
			if err := newSession.Login(); err != nil {
				newSession.Disconnect(err.Error())
				if !errors.Is(err, context.Canceled) {
					logger.Error("failed to login session", "err", err)
				}
			}
		}()
	}
	logger.Info("accepted session")
	return newSession, nil
}

// Discovery returns the server discovery instance.
func (s *Spectrum) Discovery() server.Discovery {
	return s.discovery
}

// Opts returns the configuration options.
func (s *Spectrum) Opts() util.Opts {
	return s.opts
}

// Listener returns the listener instance.
func (s *Spectrum) Listener() *minecraft.Listener {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.listeners) == 0 {
		return nil
	}
	return s.listeners[0]
}

// Listeners returns a snapshot of the public listeners. Listener returns the
// first listener for compatibility with existing single-RakNet consumers.
func (s *Spectrum) Listeners() []*minecraft.Listener {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.listeners)
}

// Registry returns the session registry instance.
func (s *Spectrum) Registry() *session.Registry {
	return s.registry
}

// Transport returns the transport instance.
func (s *Spectrum) Transport() tr.Transport {
	return s.transport
}

// Close closes the listener and stops listening for incoming connections.
func (s *Spectrum) Close() error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		<-s.closed
		return s.closeErr
	}
	s.closing = true
	close(s.done)
	listeners := slices.Clone(s.listeners)
	sessions := make([]*session.Session, 0, len(s.sessions))
	for active := range s.sessions {
		sessions = append(sessions, active)
	}
	s.mu.Unlock()
	var err error
	for _, listener := range listeners {
		err = errors.Join(err, listener.Close())
	}
	for _, activeSession := range sessions {
		activeSession.Disconnect(s.opts.ShutdownMessage)
	}
	s.acceptWG.Wait()
	s.loginWG.Wait()
	s.sessionWG.Wait()
	s.closeErr = err
	close(s.closed)
	return err
}
