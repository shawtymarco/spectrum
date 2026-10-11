package frontend

import (
	"context"
	"crypto/ecdsa"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/df-mc/go-nethernet"
	"github.com/df-mc/go-nethernet/endpoint"
	"github.com/pion/ice/v4"
	"github.com/pion/webrtc/v4"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol"
)

// NetherNetConfig separates TCP signaling, ICE UDP and operator identity. The
// consumer must load Identity from persistent storage; no ephemeral key is made.
type NetherNetConfig struct {
	SignalingAddress string
	ICEAddress       string
	// PublicIPs rewrites host candidates for a 1:1 NAT with the same UDP port.
	// Empty uses the bound interface candidates (suitable for direct hosts).
	PublicIPs          []string
	Identity           *ecdsa.PrivateKey
	TLS                *tls.Config
	Limits             Limits
	MaxSignaling       int
	NegotiationTimeout time.Duration
	// AllowAnonymous is for offline integration tests/custom private endpoints.
	// It cannot be enabled while Minecraft authentication is enabled.
	AllowAnonymous bool
}

// NetherNet returns a Spectrum.ListenWith factory owning HTTP, ICE and all peers.
func NetherNet(conf NetherNetConfig) func(minecraft.ListenConfig) (*minecraft.Listener, error) {
	if conf.MaxSignaling <= 0 {
		conf.MaxSignaling = 64
	}
	if conf.NegotiationTimeout <= 0 {
		conf.NegotiationTimeout = 10 * time.Second
	}
	return func(cfg minecraft.ListenConfig) (*minecraft.Listener, error) {
		if conf.Identity == nil {
			return nil, errors.New("persistent NetherNet operator identity is required")
		}
		if conf.SignalingAddress == "" || conf.ICEAddress == "" {
			return nil, errors.New("signaling and ICE bind addresses are required")
		}
		if conf.AllowAnonymous && !cfg.AuthenticationDisabled {
			return nil, errors.New("anonymous NetherNet requires disabled Minecraft authentication")
		}
		for _, ip := range conf.PublicIPs {
			if net.ParseIP(ip) == nil {
				return nil, fmt.Errorf("invalid ICE public IP %q", ip)
			}
		}
		limits := conf.Limits.defaults()
		tcp, err := net.Listen("tcp", conf.SignalingAddress)
		if err != nil {
			return nil, err
		}
		addr, err := net.ResolveUDPAddr("udp", conf.ICEAddress)
		if err != nil {
			_ = tcp.Close()
			return nil, err
		}
		udp, err := net.ListenUDP("udp", addr)
		if err != nil {
			_ = tcp.Close()
			return nil, err
		}
		mux := ice.NewUDPMuxDefault(ice.UDPMuxParams{UDPConn: udp})
		var settings webrtc.SettingEngine
		settings.SetICEUDPMux(mux)
		settings.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4, webrtc.NetworkTypeUDP6})
		settings.SetIncludeLoopbackCandidate(true)
		if len(conf.PublicIPs) > 0 {
			settings.SetNAT1To1IPs(conf.PublicIPs, webrtc.ICECandidateTypeHost)
		}
		ctx, cancel := context.WithCancel(context.Background())
		timeout := func(parent context.Context) (context.Context, context.CancelFunc) {
			return context.WithTimeout(parent, conf.NegotiationTimeout)
		}
		logger := cfg.ErrorLog
		if logger == nil {
			logger = slog.Default()
		}
		// endpoint.Handler logs an SDP offer on negotiation failure. Offers may
		// contain identity JWTs; retain the error, never the credential payload.
		signalingLog := slog.New(offerRedactor{logger.Handler()})
		handler := (endpoint.HandlerConfig{Logger: signalingLog, NegotiationContext: timeout, DisablePongData: true}).New()
		signaling := &ownedSignaling{Handler: handler, ctx: ctx, addr: tcp.Addr()}
		provider := cfg.StatusProvider
		if provider == nil {
			provider = minecraft.NewStatusProvider("Minecraft Server", "Spectrum")
		}
		if cfg.ListenerGroup == nil {
			cfg.ListenerGroup = new(minecraft.ListenerGroup)
		}
		group := cfg.ListenerGroup
		sem := make(chan struct{}, conf.MaxSignaling)
		work := new(httpWork)
		server := &http.Server{ReadHeaderTimeout: 5 * time.Second, ReadTimeout: conf.NegotiationTimeout + 5*time.Second,
			WriteTimeout: conf.NegotiationTimeout + 5*time.Second, IdleTimeout: 30 * time.Second,
			BaseContext: func(net.Listener) context.Context { return ctx }}
		server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !work.begin() {
				http.Error(w, "shutting down", http.StatusServiceUnavailable)
				return
			}
			defer work.wg.Done()
			select {
			case <-ctx.Done():
				http.Error(w, "shutting down", http.StatusServiceUnavailable)
				return
			default:
			}
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			default:
				http.Error(w, "signaling busy", http.StatusServiceUnavailable)
				return
			}
			status := provider.ServerStatus(group.PlayerCount(), cfg.MaximumPlayers)
			if status.MaxPlayers == 0 {
				status.MaxPlayers = status.PlayerCount + 1
			}
			handler.Status(endpoint.Status{ServerName: status.ServerName, LevelName: status.ServerSubName, PlayerCount: status.PlayerCount,
				MaxPlayerCount: status.MaxPlayers, GameType: int(status.GameType), Protocol: int(protocol.CurrentProtocol), Version: protocol.CurrentVersion})
			handler.ServeHTTP(w, r)
		})
		done := make(chan struct{})
		var once sync.Once
		var closeErr error
		cleanup := func() error {
			once.Do(func() {
				cancel()
				work.stop()
				closeErr = server.Close()
				_ = tcp.Close()
				<-done
				work.wg.Wait()
				closeErr = errors.Join(closeErr, mux.Close())
			})
			return closeErr
		}
		go func() {
			defer close(done)
			var listener net.Listener = &boundedHTTPListener{Listener: tcp, slots: make(chan struct{}, conf.MaxSignaling)}
			if conf.TLS != nil {
				listener = tls.NewListener(listener, conf.TLS.Clone())
			}
			_ = server.Serve(listener)
			cancel()
		}()
		network := minecraft.NetherNet{Signaling: signaling, ListenConfig: nethernet.ListenConfig{
			API: webrtc.NewAPI(webrtc.WithSettingEngine(settings)), AllowAnonymous: conf.AllowAnonymous, Log: cfg.ErrorLog,
			DisableTrickleICE: true, NegotiationContext: timeout,
			ConnContext: func(parent context.Context, _ *nethernet.Conn) (context.Context, context.CancelFunc) {
				return timeout(parent)
			},
			IssueServerIdentity: func(context.Context) (*nethernet.Identity, error) {
				return nethernet.GenerateServerIdentity(conf.Identity, "self")
			},
		}}
		cfg.MaximumMTU = 0
		cfg.LoginTimeout = limits.LoginTimeout
		listener, err := cfg.ListenNetwork(&ownedNetwork{Network: network, limits: limits, cleanup: cleanup}, conf.SignalingAddress)
		if err != nil {
			_ = cleanup()
			return nil, err
		}
		return listener, nil
	}
}

type ownedSignaling struct {
	*endpoint.Handler
	ctx  context.Context
	addr net.Addr
}

func (s *ownedSignaling) Context() context.Context { return s.ctx }
func (s *ownedSignaling) Addr() net.Addr           { return s.addr }
