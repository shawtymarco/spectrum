package session

import (
	"errors"
	"fmt"
	"time"

	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

const (
	subChunkOffsetBurstCapacity = 4096
	subChunkOffsetRefillPerSec  = 2048
)

var errClientPacketRateLimited = errors.New("client packet rate limit exceeded")

type packetLimiter struct {
	subChunkOffsets float64
	lastRefill      time.Time
}

func newPacketLimiter(now time.Time) packetLimiter {
	return packetLimiter{subChunkOffsets: subChunkOffsetBurstCapacity, lastRefill: now}
}

func (l *packetLimiter) allow(pk packet.Packet, now time.Time) error {
	request, ok := pk.(*packet.SubChunkRequest)
	if !ok {
		return nil
	}
	elapsed := now.Sub(l.lastRefill).Seconds()
	if elapsed > 0 {
		l.subChunkOffsets = min(subChunkOffsetBurstCapacity, l.subChunkOffsets+elapsed*subChunkOffsetRefillPerSec)
		l.lastRefill = now
	}
	cost := float64(len(request.Offsets))
	if cost > l.subChunkOffsets {
		return fmt.Errorf("%w: sub_chunk_offsets=%d available=%.0f", errClientPacketRateLimited, len(request.Offsets), l.subChunkOffsets)
	}
	l.subChunkOffsets -= cost
	return nil
}
