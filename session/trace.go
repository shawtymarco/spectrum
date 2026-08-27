package session

import (
	"time"

	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

const (
	traceAckTimeout = 5 * time.Second
	maxTraceAcks    = 16
)

type pendingTraceAck struct {
	id      uint64
	role    uint8
	started time.Time
}

func (s *Session) nextTraceID() uint64 {
	sequence := s.traceSequence.Add(1)
	return s.tracePrefix<<32 | sequence&0xffffffff
}

func (s *Session) queueTraceAcknowledgement(resultID uint64, role uint8) error {
	now := time.Now()
	s.traceAckMu.Lock()
	for token, pending := range s.traceAcks {
		if now.Sub(pending.started) > traceAckTimeout {
			delete(s.traceAcks, token)
		}
	}
	if len(s.traceAcks) >= maxTraceAcks {
		var oldestToken int64
		var oldest time.Time
		for token, pending := range s.traceAcks {
			if oldest.IsZero() || pending.started.Before(oldest) {
				oldest, oldestToken = pending.started, token
			}
		}
		delete(s.traceAcks, oldestToken)
	}
	token := int64(s.traceAckSequence.Add(1)&0x7fffffff) | int64(s.tracePrefix&0x7fffffff)<<31
	if token == 0 {
		token = 1
	}
	s.traceAcks[token] = pendingTraceAck{id: resultID, role: role, started: now}
	s.traceAckMu.Unlock()
	return s.client.WritePacket(&packet.NetworkStackLatency{Timestamp: token, NeedsResponse: true})
}

func (s *Session) completeTraceAcknowledgement(timestamp int64) (TraceAck, bool) {
	s.traceAckMu.Lock()
	pending, ok := s.traceAcks[timestamp]
	if ok {
		delete(s.traceAcks, timestamp)
	}
	s.traceAckMu.Unlock()
	if !ok {
		return TraceAck{}, false
	}
	return TraceAck{ID: pending.id, Role: pending.role, Duration: time.Since(pending.started)}, true
}
