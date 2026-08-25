package session

import (
	"testing"

	"github.com/cooldogedev/spectrum/util"
	"github.com/sandertv/gophertunnel/minecraft"
)

type protocolWithID struct {
	minecraft.Protocol
	id int32
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
