package session

import (
	"bytes"
	"testing"

	"github.com/cooldogedev/spectrum/server"
	"github.com/google/uuid"
	"github.com/sandertv/gophertunnel/minecraft/protocol"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

type retiredBackendProcessor struct {
	NopProcessor
	packets int
	swap    func()
}

func (p *retiredBackendProcessor) process(ctx *Context) {
	p.packets++
	if p.swap != nil {
		p.swap()
	} else {
		// Keep this test at the forwarding boundary without needing a socket.
		ctx.Cancel()
	}
}

func (p *retiredBackendProcessor) ProcessServer(ctx *Context, _ *packet.Packet) {
	p.process(ctx)
}

func (p *retiredBackendProcessor) ProcessServerEncoded(ctx *Context, _ *[]byte) {
	p.process(ctx)
}

func TestRetiredBackendCannotRemoveDestinationPlayers(t *testing.T) {
	for _, encoded := range []bool{false, true} {
		name := "decoded"
		if encoded {
			name = "encoded"
		}
		t.Run(name, func(t *testing.T) {
			old, current := &server.Conn{}, &server.Conn{}
			processor := &retiredBackendProcessor{}
			s := &Session{serverConn: current, processor: processor, tracker: newTracker()}
			id := uuid.New()
			for _, pk := range []packet.Packet{
				&packet.PlayerList{Entries: []protocol.PlayerListEntry{{ActionType: protocol.PlayerListActionRemove, UUID: id}}},
				&packet.RemoveActor{EntityUniqueID: 42},
			} {
				// A read may have captured old immediately before dial publishes
				// current. Its successful result must not reach current's client.
				forwardBackendFixture(t, s, old, pk, encoded)
			}
			if processor.packets != 0 {
				t.Fatalf("retired backend delivered %d player removals after replacement", processor.packets)
			}
			forwardBackendFixture(t, s, current, &packet.AddPlayer{UUID: id, EntityRuntimeID: 42}, encoded)
			if processor.packets != 1 {
				t.Fatal("current backend packet did not reach the processor")
			}
		})
	}
}

func forwardBackendFixture(t *testing.T, s *Session, backend *server.Conn, pk packet.Packet, encoded bool) {
	t.Helper()
	var err error
	var forwarded bool
	if encoded {
		var buf bytes.Buffer
		if err := (&packet.Header{PacketID: pk.ID()}).Write(&buf); err != nil {
			t.Fatal(err)
		}
		pk.Marshal(protocol.NewWriter(&buf, -1))
		forwarded, err = handleEncodedServerPacket(s, backend, buf.Bytes())
	} else {
		forwarded, err = handleServerPacket(s, backend, pk)
	}
	if err != nil {
		t.Fatal(err)
	}
	if forwarded {
		t.Fatal("cancelled or retired packet was reported as delivered")
	}
}

func TestBackendReplacementDuringProcessorDoesNotWriteRetiredPacket(t *testing.T) {
	for _, encoded := range []bool{false, true} {
		old, current := &server.Conn{}, &server.Conn{}
		s := &Session{serverConn: old, tracker: newTracker()}
		s.processor = &retiredBackendProcessor{swap: func() {
			// Processor callbacks may initiate a transfer. They must run outside
			// serverMu, followed by another ownership check before forwarding.
			s.serverMu.Lock()
			s.serverConn = current
			s.serverMu.Unlock()
		}}
		forwardBackendFixture(t, s, old, &packet.RemoveActor{EntityUniqueID: 42}, encoded)
		// The intentionally absent public connection makes an erroneous write
		// fail immediately instead of silently accepting the obsolete removal.
	}
}
