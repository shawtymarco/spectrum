package packet

import (
	"bytes"
	"testing"

	"github.com/sandertv/gophertunnel/minecraft/protocol"
)

func TestTracedPacketRoundTrip(t *testing.T) {
	want := &TracedPacket{Version: 1, TraceID: 42, Payload: []byte{1, 2, 3}}
	buffer := bytes.NewBuffer(nil)
	want.Marshal(protocol.NewWriter(buffer, 0))
	got := &TracedPacket{}
	got.Marshal(protocol.NewReader(buffer, 0, false))
	if got.Version != want.Version || got.TraceID != want.TraceID || !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("round trip = %#v, want %#v", got, want)
	}
}

func TestTraceResultRoundTrip(t *testing.T) {
	want := &TraceResult{
		Version: 1, TraceID: 7, Accepted: true, Terminal: true, FeedbackComplete: true,
		Role: 2, Reason: "accepted", QueueNanoseconds: 11, HandleNanoseconds: 13,
	}
	buffer := bytes.NewBuffer(nil)
	want.Marshal(protocol.NewWriter(buffer, 0))
	got := &TraceResult{}
	got.Marshal(protocol.NewReader(buffer, 0, false))
	if *got != *want {
		t.Fatalf("round trip = %#v, want %#v", got, want)
	}
}
