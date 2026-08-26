package session

import (
	"errors"
	"testing"
	"time"

	"github.com/sandertv/gophertunnel/minecraft/protocol"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

func TestPacketLimiterAllowsObservedTransferBurst(t *testing.T) {
	now := time.Unix(0, 0)
	limiter := newPacketLimiter(now)
	request := &packet.SubChunkRequest{Offsets: make([]protocol.SubChunkOffset, 1067)}
	for range 3 {
		if err := limiter.allow(request, now); err != nil {
			t.Fatalf("legitimate transfer burst was rejected: %v", err)
		}
	}
	if err := limiter.allow(request, now); !errors.Is(err, errClientPacketRateLimited) {
		t.Fatalf("repeated burst error: got %v, want rate limit", err)
	}
}

func TestPacketLimiterRefillsWithoutExceedingCapacity(t *testing.T) {
	now := time.Unix(0, 0)
	limiter := newPacketLimiter(now)
	maximum := &packet.SubChunkRequest{Offsets: make([]protocol.SubChunkOffset, subChunkOffsetBurstCapacity)}
	if err := limiter.allow(maximum, now); err != nil {
		t.Fatal(err)
	}
	half := &packet.SubChunkRequest{Offsets: make([]protocol.SubChunkOffset, subChunkOffsetRefillPerSec)}
	if err := limiter.allow(half, now.Add(time.Second)); err != nil {
		t.Fatalf("refilled request was rejected: %v", err)
	}
	if err := limiter.allow(maximum, now.Add(10*time.Second)); err != nil {
		t.Fatalf("capacity did not refill: %v", err)
	}
}
