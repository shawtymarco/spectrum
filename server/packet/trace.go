package packet

import "github.com/sandertv/gophertunnel/minecraft/protocol"

// TracedPacket carries one native Bedrock packet over Spectrum's internal
// transport together with an opaque trace identifier. It is never forwarded to
// a public Bedrock client.
type TracedPacket struct {
	Version uint8
	TraceID uint64
	Payload []byte
}

func (*TracedPacket) ID() uint32 { return IDTracedPacket }

func (pk *TracedPacket) Marshal(io protocol.IO) {
	io.Uint8(&pk.Version)
	io.Uint64(&pk.TraceID)
	io.ByteSlice(&pk.Payload)
}

// TraceResult is an internal ordered marker emitted after gameplay has queued
// the feedback associated with a traced packet.
type TraceResult struct {
	Version           uint8
	TraceID           uint64
	Accepted          bool
	Terminal          bool
	FeedbackComplete  bool
	Role              uint8
	Reason            string
	QueueNanoseconds  int64
	HandleNanoseconds int64
}

func (*TraceResult) ID() uint32 { return IDTraceResult }

func (pk *TraceResult) Marshal(io protocol.IO) {
	io.Uint8(&pk.Version)
	io.Uint64(&pk.TraceID)
	io.Bool(&pk.Accepted)
	io.Bool(&pk.Terminal)
	io.Bool(&pk.FeedbackComplete)
	io.Uint8(&pk.Role)
	io.String(&pk.Reason)
	io.Int64(&pk.QueueNanoseconds)
	io.Int64(&pk.HandleNanoseconds)
}
