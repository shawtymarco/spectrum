package server

import (
	"bytes"
	"context"
	"net"
	"testing"

	spectrumprotocol "github.com/cooldogedev/spectrum/protocol"
	spectrumpacket "github.com/cooldogedev/spectrum/server/packet"
	"github.com/golang/snappy"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

func TestWriteTracedUsesOneInternalFrame(t *testing.T) {
	client, backend := net.Pipe()
	defer client.Close()
	defer backend.Close()
	c := &Conn{
		ctx:      context.Background(),
		writer:   spectrumprotocol.NewWriter(client),
		protocol: minecraft.DefaultProtocol,
	}

	written := make(chan error, 1)
	go func() { written <- c.WriteTraced([]byte{1, 2, 3}, 42) }()
	frame, err := spectrumprotocol.NewReader(backend).ReadPacket()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := snappy.Decode(nil, frame)
	if err != nil {
		t.Fatal(err)
	}
	buffer := bytes.NewBuffer(decoded)
	header := &packet.Header{}
	if err := header.Read(buffer); err != nil {
		t.Fatal(err)
	}
	traced := &spectrumpacket.TracedPacket{}
	traced.Marshal(protocol.NewReader(buffer, 0, false))
	if header.PacketID != spectrumpacket.IDTracedPacket || traced.TraceID != 42 || !bytes.Equal(traced.Payload, []byte{1, 2, 3}) {
		t.Fatalf("traced frame = id:%d packet:%#v", header.PacketID, traced)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
}
