package server

import (
	"testing"

	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

type preSpawnProtocol struct{ minecraft.Protocol }

func (preSpawnProtocol) PreSpawnPackets() []packet.Packet {
	return []packet.Packet{&packet.BiomeDefinitionList{}}
}

func TestDiscardBackendBootstrapPacket(t *testing.T) {
	legacy := preSpawnProtocol{Protocol: minecraft.DefaultProtocol}
	if !discardBackendBootstrapPacket(&packet.BiomeDefinitionList{}, legacy) {
		t.Fatal("legacy duplicate biome definitions were forwarded")
	}
	if discardBackendBootstrapPacket(&packet.BiomeDefinitionList{}, minecraft.DefaultProtocol) {
		t.Fatal("native biome definitions were discarded")
	}
	if discardBackendBootstrapPacket(&packet.CreativeContent{}, legacy) {
		t.Fatal("authoritative creative content was discarded")
	}
}
