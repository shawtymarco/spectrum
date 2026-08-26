package session

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	spectrumserver "github.com/cooldogedev/spectrum/server"
	spectrumpacket "github.com/cooldogedev/spectrum/server/packet"
	"github.com/cooldogedev/spectrum/util"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

// handleServer continuously reads packets from the server and forwards them to the client.
func handleServer(s *Session) {
loop:
	for {
		select {
		case <-s.ctx.Done():
			s.CloseWithError(context.Cause(s.ctx))
			break loop
		default:
		}

		server := s.Server()
		pk, err := server.ReadPacket()
		if err != nil {
			current, backendAddr := s.backendIsCurrent(server)
			if !current {
				s.logger.Debug("retired backend stream closed during transfer", "err", err)
				continue loop
			}

			s.logger.Warn("active backend stream read failed; attempting fallback", "backend", backendAddr, "err", err)
			server.CloseWithError(fmt.Errorf("failed to read packet from server: %w", err))
			if err := s.fallback(); err != nil {
				s.CloseWithError(fmt.Errorf("fallback failed: %w", err))
				break loop
			}
			continue loop
		}

		switch pk := pk.(type) {
		case *spectrumpacket.Flush:
			ctx := NewContext()
			s.Processor().ProcessFlush(ctx)
			if ctx.Cancelled() {
				continue loop
			}

			if err := s.client.Flush(); err != nil {
				s.CloseWithError(fmt.Errorf("failed to flush client's buffer: %w", err))
				logError(s, "failed to flush client's buffer", err)
				break loop
			}
		case *spectrumpacket.Latency:
			s.latency.Store(pk.Latency)
		case *spectrumpacket.Transfer:
			if err := s.Transfer(pk.Addr); err != nil {
				logError(s, "failed to transfer", err)
			}
		case *spectrumpacket.UpdateCache:
			s.SetCache(pk.Cache)
		case packet.Packet:
			if err := handleServerPacket(s, pk); err != nil {
				s.CloseWithError(fmt.Errorf("failed to write packet to client: %w", err))
				logError(s, "failed to write packet to client", err)
				break loop
			}
		case []byte:
			ctx := NewContext()
			s.Processor().ProcessServerEncoded(ctx, &pk)
			if ctx.Cancelled() {
				continue loop
			}

			if _, err := s.client.Write(pk); err != nil {
				s.CloseWithError(fmt.Errorf("failed to write packet to client: %w", err))
				logError(s, "failed to write packet to client", err)
				break loop
			}
		}
	}
}

// handleClient continuously reads packets from the client and forwards them to the server.
func handleClient(s *Session) {
	header := &packet.Header{}
	pool := s.client.Proto().Packets(true)
	var shieldID int32
	for _, item := range s.client.GameData().Items {
		if item.Name == "minecraft:shield" {
			shieldID = int32(item.RuntimeID)
			break
		}
	}

loop:
	for {
		select {
		case <-s.ctx.Done():
			s.CloseWithError(context.Cause(s.ctx))
			break loop
		default:
		}

		payload, err := s.client.ReadBytes()
		if err != nil {
			s.CloseWithError(fmt.Errorf("failed to read packet from client: %w", err))
			logError(s, "failed to read packet from client", err)
			break loop
		}

		backend := s.Server()
		if err := handleClientPacket(s, backend, header, pool, shieldID, payload); err != nil {
			if errors.Is(err, errClientPacketRateLimited) {
				s.logger.Warn("disconnecting rate-limited client", "err", err)
				s.CloseWithError(err)
				break loop
			}
			current, backendAddr := s.backendIsCurrent(backend)
			if !current {
				// A transfer may retire the backend while a client packet write is
				// already in flight. Never close the newly published backend for an
				// error returned by that old stream.
				s.logger.Debug("ignored client write failure from retired backend", "err", err)
				continue loop
			}
			s.logger.Error("active backend client write failed", "backend", backendAddr, "err", err)
			backend.CloseWithError(fmt.Errorf("failed to write packet to server: %w", err))
		}
	}
}

// handleLatency periodically sends the client's current ping and timestamp to the server for latency reporting.
// The client's latency is derived from half of RakNet's round-trip time (RTT).
// To calculate the total latency, we multiply this value by 2.
func handleLatency(s *Session, interval int64) {
	ticker := time.NewTicker(time.Millisecond * time.Duration(interval))
	defer ticker.Stop()
loop:
	for {
		select {
		case <-s.ctx.Done():
			s.CloseWithError(context.Cause(s.ctx))
			break loop
		case <-ticker.C:
			if err := s.Server().WritePacket(&spectrumpacket.Latency{Latency: s.client.Latency().Milliseconds() * 2, Timestamp: time.Now().UnixMilli()}); err != nil {
				logError(s, "failed to write latency packet", err)
			}
		}
	}
}

// handleServerPacket processes and forwards the provided packet from the server to the client.
func handleServerPacket(s *Session, pk packet.Packet) (err error) {
	ctx := NewContext()
	s.Processor().ProcessServer(ctx, &pk)
	if ctx.Cancelled() {
		return
	}

	if s.opts.SyncProtocol {
		for _, latest := range s.client.Proto().ConvertToLatest(pk, s.client) {
			s.tracker.handlePacket(latest)
		}
	} else {
		s.tracker.handlePacket(pk)
	}
	return s.client.WritePacket(pk)
}

// handleClientPacket processes and forwards the provided packet from the client to the server.
func handleClientPacket(s *Session, backend *spectrumserver.Conn, header *packet.Header, pool packet.Pool, shieldID int32, payload []byte) (err error) {
	ctx := NewContext()
	buf := bytes.NewBuffer(payload)
	if err := header.Read(buf); err != nil {
		return errors.New("failed to decode header")
	}

	if !shouldDecodeClientPacket(s.client.Proto(), s.opts, header.PacketID) {
		s.Processor().ProcessClientEncoded(ctx, &payload)
		if !ctx.Cancelled() {
			return backend.Write(payload)
		}
		return
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic while decoding packet %v: %v", header.PacketID, r)
		}
	}()

	factory, ok := pool[header.PacketID]
	if !ok {
		return fmt.Errorf("unknown packet %d", header.PacketID)
	}

	pk := factory()
	pk.Marshal(s.client.Proto().NewReader(buf, shieldID, true))
	if s.opts.SyncProtocol {
		s.Processor().ProcessClient(ctx, &pk)
		if ctx.Cancelled() {
			return
		}
		return backend.WritePacket(pk)
	}

	for _, latest := range s.client.Proto().ConvertToLatest(pk, s.client) {
		if err := s.limiter.allow(latest, time.Now()); err != nil {
			return err
		}
		s.Processor().ProcessClient(ctx, &latest)
		if ctx.Cancelled() {
			break
		}

		if err := backend.WritePacket(latest); err != nil {
			return err
		}
	}
	return
}

// shouldDecodeClientPacket reports whether a client packet must cross the
// minecraft.Protocol conversion boundary before it is sent to a backend. A
// historical client cannot use the raw fast path while the backend wire stays
// native: doing so would forward the historical packet layout unchanged.
func shouldDecodeClientPacket(proto minecraft.Protocol, opts util.Opts, packetID uint32) bool {
	if slices.Contains(opts.ClientDecode, packetID) {
		return true
	}
	return !opts.SyncProtocol && proto.ID() != minecraft.DefaultProtocol.ID()
}

func logError(s *Session, msg string, err error) {
	select {
	case <-s.ctx.Done():
		return
	default:
	}

	if !errors.Is(err, context.Canceled) {
		s.logger.Error(msg, "err", err)
	}
}
