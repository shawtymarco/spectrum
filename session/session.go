package session

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cooldogedev/spectrum/server"
	"github.com/cooldogedev/spectrum/session/animation"
	"github.com/cooldogedev/spectrum/transport"
	"github.com/cooldogedev/spectrum/util"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

// Session represents a player session within the proxy, managing client and server interactions,
// including transfers, fallbacks, and tracking various session states.
type Session struct {
	ctx        context.Context
	cancelFunc context.CancelCauseFunc

	client *minecraft.Conn

	serverAddr string
	serverConn *server.Conn
	serverMu   sync.RWMutex

	logger   *slog.Logger
	registry *Registry

	discovery server.Discovery
	opts      util.Opts
	transport transport.Transport

	animation animation.Animation
	tracker   *tracker

	processor   Processor
	processorMu sync.RWMutex
	readyMu     sync.Mutex
	ready       *readyTransfer

	cache      atomic.Value
	latency    atomic.Int64
	inFallback atomic.Bool
	once       sync.Once

	tracePrefix            uint64
	traceSequence          atomic.Uint64
	traceAckSequence       atomic.Uint64
	spectatorTraceSequence atomic.Uint64
	traceAckMu             sync.Mutex
	traceAcks              map[int64]pendingTraceAck
}

var errFallbackInProgress = errors.New("fallback already in progress")
var nextTracePrefix atomic.Uint64

const backendReadyTimeout = 15 * time.Second

type readyTransferPhase uint8

const (
	readyTransferWaiting readyTransferPhase = iota
	readyTransferStreaming
)

type readyTransfer struct {
	backend *server.Conn
	origin  string
	target  string
	data    minecraft.GameData
	phase   readyTransferPhase
	timer   *time.Timer
}

// NewSession creates a new Session instance using the provided minecraft.Conn.
func NewSession(client *minecraft.Conn, logger *slog.Logger, registry *Registry, discovery server.Discovery, opts util.Opts, transport transport.Transport) *Session {
	s := &Session{
		client: client,

		logger:   logger,
		registry: registry,

		discovery: discovery,
		opts:      opts,
		transport: transport,

		processor: NopProcessor{},

		animation:   &animation.Dimension{},
		tracker:     newTracker(),
		tracePrefix: nextTracePrefix.Add(1),
		traceAcks:   make(map[int64]pendingTraceAck),
	}
	s.ctx, s.cancelFunc = context.WithCancelCause(client.Context())
	s.cache.Store([]byte(nil))
	return s
}

// Login initiates the login sequence with a default timeout of 1 minute.
func (s *Session) Login() (err error) {
	ctx, cancel := context.WithTimeout(s.ctx, time.Minute)
	defer cancel()
	return s.LoginContext(ctx)
}

// LoginTimeout initiates the login sequence with the specified timeout duration.
func (s *Session) LoginTimeout(duration time.Duration) (err error) {
	ctx, cancel := context.WithTimeout(s.ctx, duration)
	defer cancel()
	return s.LoginContext(ctx)
}

// LoginContext initiates the login sequence for the session, including server discovery,
// establishing a connection, and spawning the player in the game. The process is performed
// using the provided context for cancellation.
func (s *Session) LoginContext(ctx context.Context) (err error) {
	identityData := s.client.IdentityData()
	serverAddr, err := s.discovery.Discover(s.client)
	if err != nil {
		s.logger.Debug("discovery failed", "err", err)
		return err
	}

	conn, err := s.dial(ctx, serverAddr)
	if err != nil {
		s.logger.Debug("dialer failed", "err", err)
		return err
	}

	go handleServer(s)
	go handleClient(s)
	go handleLatency(s, s.opts.LatencyInterval)
	if err := conn.DoConnect(); err != nil {
		s.logger.Debug("connection sequence failed", "err", err)
		return err
	}

	if err := conn.WaitConnect(ctx); err != nil {
		conn.CloseWithError(fmt.Errorf("connection sequence failed: %w", err))
		s.logger.Debug("connection sequence failed", "err", err)
		return err
	}

	gameData := conn.GameData()
	s.Processor().ProcessStartGame(NewContext(), &gameData)
	if err := s.client.StartGame(gameData); err != nil {
		s.logger.Debug("startgame sequence failed", "err", err)
		return err
	}

	if err := conn.DoSpawn(); err != nil {
		s.logger.Debug("spawn sequence failed", "err", err)
		return err
	}
	s.registry.AddSession(identityData.XUID, s)
	s.logger.Info("logged in session")
	return
}

// Transfer initiates a transfer to a different server using the specified address.
// It sets a default timeout of 1 minute for the transfer operation.
func (s *Session) Transfer(addr string) (err error) {
	ctx, cancel := context.WithTimeout(s.ctx, time.Minute)
	defer cancel()
	return s.transferContext(ctx, addr, false)
}

// TransferReady initiates a transfer whose target must publish BackendReady.
// Packets produced by the target before that marker are discarded.
func (s *Session) TransferReady(addr string) (err error) {
	ctx, cancel := context.WithTimeout(s.ctx, time.Minute)
	defer cancel()
	return s.transferContext(ctx, addr, true)
}

// TransferTimeout initiates a transfer to a different server using the specified address
// and a custom timeout duration for the transfer operation.
func (s *Session) TransferTimeout(addr string, duration time.Duration) (err error) {
	ctx, cancel := context.WithTimeout(s.ctx, duration)
	defer cancel()
	return s.transferContext(ctx, addr, false)
}

// TransferContext initiates a transfer to a different server using the specified address. It ensures that only one transfer
// occurs at a time, returning an error if another transfer is already in progress.
// The process is performed using the provided context for cancellation.
func (s *Session) TransferContext(ctx context.Context, addr string) (err error) {
	return s.transferContext(ctx, addr, false)
}

func (s *Session) transferContext(ctx context.Context, addr string, waitForReady bool) (err error) {
	s.serverMu.RLock()
	origin := s.serverAddr
	s.serverMu.RUnlock()
	processorCtx := NewContext()
	s.Processor().ProcessPreTransfer(processorCtx, &origin, &addr)
	if processorCtx.Cancelled() {
		return errors.New("processor failed")
	}

	s.sendMetadata(true)
	conn, err := s.dial(ctx, addr)
	if err != nil {
		s.Processor().ProcessTransferFailure(NewContext(), &origin, &addr, err)
		return fmt.Errorf("dialer failed: %w", err)
	}

	if err := conn.DoConnect(); err != nil {
		s.Processor().ProcessTransferFailure(NewContext(), &origin, &addr, err)
		return fmt.Errorf("connection sequence failed failed: %w", err)
	}

	conn.OnConnect(func(err error) {
		if err != nil {
			s.inFallback.Store(false)
			s.Processor().ProcessTransferFailure(NewContext(), &origin, &addr, err)
			return
		}

		gameData := conn.GameData()
		s.animation.Play(s.client, gameData)
		s.sendGameData(conn.GameData())
		if waitForReady {
			s.installReadyTransfer(conn, origin, addr, gameData)
		}
		if err := conn.DoSpawn(); err != nil {
			s.cancelReadyTransfer(conn)
			s.inFallback.Store(false)
			s.Processor().ProcessTransferFailure(NewContext(), &origin, &addr, err)
			return
		}
		if waitForReady {
			s.logger.Debug("waiting for backend ready", "origin", origin, "target", addr)
			return
		}
		s.inFallback.Store(false)
		s.animation.Clear(s.client, gameData)
		s.Processor().ProcessPostTransfer(NewContext(), &origin, &addr)
		s.logger.Debug("transferred session", "origin", origin, "target", addr)
	})
	return nil
}

func (s *Session) installReadyTransfer(backend *server.Conn, origin, target string, data minecraft.GameData) {
	state := &readyTransfer{backend: backend, origin: origin, target: target, data: data, phase: readyTransferWaiting}
	state.timer = time.AfterFunc(backendReadyTimeout, func() {
		if !s.expireReadyTransfer(backend) {
			return
		}
		backend.CloseWithError(fmt.Errorf("backend ready timeout after %s", backendReadyTimeout))
	})

	s.readyMu.Lock()
	previous := s.ready
	s.ready = state
	s.readyMu.Unlock()
	if previous != nil && previous.timer != nil {
		previous.timer.Stop()
	}
}

func (s *Session) markBackendReady(backend *server.Conn) error {
	s.readyMu.Lock()
	state := s.ready
	if state == nil || state.backend != backend {
		s.readyMu.Unlock()
		return errors.New("backend ready received without a pending transfer")
	}
	if state.phase != readyTransferWaiting {
		s.readyMu.Unlock()
		return errors.New("backend ready received more than once")
	}
	state.phase = readyTransferStreaming
	data := state.data
	target := state.target
	s.readyMu.Unlock()

	animation.BeginClear(s.animation, s.client, data)
	s.logger.Debug("backend ready received; waiting for first chunk", "target", target)
	return nil
}

func (s *Session) backendWaiting(backend *server.Conn) bool {
	s.readyMu.Lock()
	defer s.readyMu.Unlock()
	return s.ready != nil && s.ready.backend == backend && s.ready.phase == readyTransferWaiting
}

func (s *Session) completeReadyTransfer(backend *server.Conn) {
	s.readyMu.Lock()
	state := s.ready
	if state == nil || state.backend != backend || state.phase != readyTransferStreaming {
		s.readyMu.Unlock()
		return
	}
	s.ready = nil
	if state.timer != nil {
		state.timer.Stop()
	}
	s.readyMu.Unlock()

	animation.EndClear(s.animation, s.client, state.data)
	_ = s.client.Flush()
	s.inFallback.Store(false)
	s.Processor().ProcessPostTransfer(NewContext(), &state.origin, &state.target)
	s.logger.Debug("transferred ready session", "origin", state.origin, "target", state.target)
}

func (s *Session) expireReadyTransfer(backend *server.Conn) bool {
	s.readyMu.Lock()
	defer s.readyMu.Unlock()
	if s.ready == nil || s.ready.backend != backend {
		return false
	}
	s.ready = nil
	return true
}

func (s *Session) cancelReadyTransfer(backend *server.Conn) {
	s.readyMu.Lock()
	state := s.ready
	if state == nil || state.backend != backend {
		s.readyMu.Unlock()
		return
	}
	s.ready = nil
	if state.timer != nil {
		state.timer.Stop()
	}
	s.readyMu.Unlock()
}

// Animation returns the animation set to be played during server transfers.
func (s *Session) Animation() animation.Animation {
	return s.animation
}

// SetAnimation sets the animation to be played during server transfers.
func (s *Session) SetAnimation(animation animation.Animation) {
	s.animation = animation
}

// Cache returns the current session cache.
func (s *Session) Cache() []byte {
	return s.cache.Load().([]byte)
}

// SetCache updates the session cache.
func (s *Session) SetCache(cache []byte) {
	ctx := NewContext()
	s.Processor().ProcessCache(ctx, &cache)
	if !ctx.Cancelled() {
		s.cache.Store(cache)
	}
}

// Processor returns the current processor.
func (s *Session) Processor() Processor {
	s.processorMu.RLock()
	defer s.processorMu.RUnlock()
	return s.processor
}

// SetProcessor sets a new processor for the session.
func (s *Session) SetProcessor(processor Processor) {
	s.processorMu.Lock()
	s.processor = processor
	s.processorMu.Unlock()
}

// Latency returns the measured end-to-end round-trip time. The backend
// response already includes the public RakNet RTT, so adding it again would
// double-count the client leg.
func (s *Session) Latency() int64 {
	return reportedLatency(s.client.Latency().Milliseconds(), s.latency.Load())
}

func reportedLatency(clientHalfRTT, backendReportedRTT int64) int64 {
	if backendReportedRTT > 0 {
		return backendReportedRTT
	}
	if clientHalfRTT < 0 {
		return 0
	}
	return clientHalfRTT * 2
}

// Client returns the client connection.
func (s *Session) Client() *minecraft.Conn {
	return s.client
}

// Server returns the current server connection.
func (s *Session) Server() *server.Conn {
	s.serverMu.RLock()
	defer s.serverMu.RUnlock()
	return s.serverConn
}

// backendIsCurrent reports whether conn is still the active backend and
// returns the active backend address from the same lock snapshot.
func (s *Session) backendIsCurrent(conn *server.Conn) (bool, string) {
	s.serverMu.RLock()
	defer s.serverMu.RUnlock()
	return s.serverConn == conn, s.serverAddr
}

// Context returns the connection's context. The context is canceled when the session is closed,
// allowing for cancellation of operations that are tied to the lifecycle of the session.
func (s *Session) Context() context.Context {
	return s.ctx
}

// Disconnect sends a packet.Disconnect to the client and closes the session.
func (s *Session) Disconnect(message string) {
	s.CloseWithError(errors.New(message))
}

// Close closes the session, including the server and client connections.
func (s *Session) Close() (err error) {
	s.CloseWithError(errors.New("closed by application"))
	return nil
}

func (s *Session) CloseWithError(err error) {
	s.once.Do(func() {
		message := err.Error()
		s.Processor().ProcessDisconnection(NewContext(), &message)
		_ = s.client.WritePacket(&packet.Disconnect{Message: message})
		_ = s.client.Close()
		if conn := s.Server(); conn != nil {
			conn.CloseWithError(err)
		}
		s.cancelFunc(err)
		s.registry.RemoveSession(s.client.IdentityData().XUID)
		s.logger.Info("closed session", "err", err)
	})
}

// dial dials the specified server address and returns a new server.Conn instance.
// The provided context is used to manage timeouts and cancellations during the dialing process.
func (s *Session) dial(ctx context.Context, addr string) (*server.Conn, error) {
	select {
	case <-s.ctx.Done():
		return nil, context.Cause(s.ctx)
	default:
	}
	if strings.TrimSpace(addr) == "" {
		return nil, errors.New("server address is empty")
	}

	transportConn, err := s.transport.Dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	c := server.NewConn(transportConn, s.client, s.logger.With("addr", addr), s.opts.SyncProtocol, s.Cache())

	// Publish the replacement before closing the previous backend. The server
	// read loop uses this pointer change to distinguish an intentional transfer
	// from a backend failure that should trigger fallback.
	s.serverMu.Lock()
	previous := s.serverConn
	s.serverAddr = addr
	s.serverConn = c
	s.serverMu.Unlock()
	if previous != nil {
		s.cancelReadyTransfer(previous)
		_ = previous.Close()
	}
	return c, nil
}

// fallback attempts to transfer the session to a fallback server provided by the discovery.
func (s *Session) fallback() (err error) {
	select {
	case <-s.ctx.Done():
		return context.Cause(s.ctx)
	default:
	}

	if !s.inFallback.CompareAndSwap(false, true) {
		return errFallbackInProgress
	}
	// Synchronous discovery/dial/setup failures never reach the connection's
	// OnConnect callback, so release the guard here and allow a later retry.
	defer func() {
		if err != nil {
			s.inFallback.Store(false)
		}
	}()

	s.serverMu.RLock()
	origin := s.serverAddr
	s.serverMu.RUnlock()
	processorCtx := NewContext()
	addr, err := s.discovery.DiscoverFallback(s.client)
	if err != nil {
		s.Processor().ProcessFallbackFailure(processorCtx, &origin, nil, err)
		return fmt.Errorf("discovery failed: %w", err)
	}
	if strings.TrimSpace(addr) == "" {
		err := errors.New("no fallback server configured")
		s.Processor().ProcessFallbackFailure(processorCtx, &origin, &addr, err)
		return err
	}

	s.Processor().ProcessPreFallback(processorCtx, &origin, &addr)
	if processorCtx.Cancelled() {
		return errors.New("processor failed")
	}

	s.logger.Debug("transferring session to a fallback server", "addr", addr)
	if err := s.Transfer(addr); err != nil {
		s.Processor().ProcessFallbackFailure(processorCtx, &origin, &addr, err)
		return fmt.Errorf("transfer failed: %w", err)
	}
	s.Processor().ProcessPostFallback(processorCtx, &origin, &addr)
	return nil
}

func (s *Session) sendMetadata(noAI bool) {
	metadata := protocol.NewEntityMetadata()
	if noAI {
		metadata.SetFlag(protocol.EntityDataKeyFlags, protocol.EntityDataFlagNoAI)
	}
	metadata.SetFlag(protocol.EntityDataKeyFlags, protocol.EntityDataFlagBreathing)
	metadata.SetFlag(protocol.EntityDataKeyFlags, protocol.EntityDataFlagHasGravity)
	_ = s.client.WritePacket(&packet.SetActorData{
		EntityRuntimeID: s.client.GameData().EntityRuntimeID,
		EntityMetadata:  metadata,
	})
}

func (s *Session) sendGameData(gameData minecraft.GameData) {
	chunk := emptyChunk(gameData.Dimension)
	pos := gameData.PlayerPosition
	chunkX := int32(pos.X()) >> 4
	chunkZ := int32(pos.Z()) >> 4
	for x := chunkX - 4; x <= chunkX+4; x++ {
		for z := chunkZ - 4; z <= chunkZ+4; z++ {
			_ = s.client.WritePacket(&packet.LevelChunk{
				Dimension:     gameData.Dimension,
				Position:      protocol.ChunkPos{x, z},
				SubChunkCount: 1,
				RawPayload:    chunk,
			})
		}
	}
	s.tracker.mu.Lock()
	s.tracker.clearEffects(s)
	s.tracker.clearEntities(s)
	s.tracker.clearBossBars(s)
	s.tracker.clearPlayers(s)
	s.tracker.clearScoreboards(s)
	s.tracker.mu.Unlock()
	_ = s.client.WritePacket(&packet.MovePlayer{
		EntityRuntimeID: gameData.EntityRuntimeID,
		Position:        gameData.PlayerPosition,
		Pitch:           gameData.Pitch,
		Yaw:             gameData.Yaw,
		Mode:            packet.MoveModeReset,
	})
	_ = s.client.WritePacket(&packet.LevelEvent{EventType: packet.LevelEventStopRaining, EventData: 10_000})
	_ = s.client.WritePacket(&packet.LevelEvent{EventType: packet.LevelEventStopThunderstorm})
	_ = s.client.WritePacket(&packet.SetDifficulty{Difficulty: uint32(gameData.Difficulty)})
	_ = s.client.WritePacket(&packet.SetPlayerGameType{GameType: gameData.PlayerGameMode})
	_ = s.client.WritePacket(&packet.GameRulesChanged{GameRules: gameData.GameRules})
}
