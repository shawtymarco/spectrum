package session

import (
	"bytes"
	"testing"

	"github.com/cooldogedev/spectrum/util"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

type protocolWithID struct {
	minecraft.Protocol
	id int32
}

func TestEncodedPacketID(t *testing.T) {
	var payload bytes.Buffer
	header := &packet.Header{PacketID: packet.IDLevelChunk}
	if err := header.Write(&payload); err != nil {
		t.Fatal(err)
	}
	if got, ok := encodedPacketID(payload.Bytes()); !ok || got != packet.IDLevelChunk {
		t.Fatalf("encoded packet ID = %d, %t", got, ok)
	}
}

func (p protocolWithID) ID() int32 { return p.id }

func TestShouldDecodeClientPacket(t *testing.T) {
	native := minecraft.DefaultProtocol
	historical := protocolWithID{Protocol: minecraft.DefaultProtocol, id: native.ID() - 1}

	tests := []struct {
		name     string
		proto    minecraft.Protocol
		opts     util.Opts
		packetID uint32
		want     bool
	}{
		{name: "native fast path", proto: native, opts: *util.DefaultOpts(), packetID: 1},
		{name: "configured native decode", proto: native, opts: util.Opts{ClientDecode: []uint32{1}}, packetID: 1, want: true},
		{name: "historical native backend", proto: historical, opts: *util.DefaultOpts(), packetID: 1, want: true},
		{name: "historical synced backend", proto: historical, opts: util.Opts{SyncProtocol: true}, packetID: 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := shouldDecodeClientPacket(test.proto, test.opts, test.packetID); got != test.want {
				t.Fatalf("shouldDecodeClientPacket() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestConfiguredClientDiscardPrecedesDecode(t *testing.T) {
	opts := util.Opts{ClientDiscard: []uint32{175}, ClientDecode: []uint32{175}}
	if !shouldDiscardClientPacket(opts, 175) {
		t.Fatal("configured client discard was ignored")
	}
	if shouldDiscardClientPacket(opts, 174) {
		t.Fatal("unconfigured client packet was discarded")
	}
}
