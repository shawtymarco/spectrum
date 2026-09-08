package session

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	framing "github.com/cooldogedev/spectrum/protocol"
	spectrumpacket "github.com/cooldogedev/spectrum/server/packet"
	"github.com/cooldogedev/spectrum/util"
	"github.com/golang/snappy"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

// TestLoginUsesProcessedShieldRegistry exercises the real public login and
// Session.LoginContext against an in-memory framed backend. A pre-StartGame
// packet proves the client pump is already running before the registry exists;
// the later equipment packet must use the processed registry, not that empty
// early snapshot or the original backend shield ID.
func TestLoginUsesProcessedShieldRegistry(t *testing.T) {
	for _, inspect := range []bool{false, true} {
		t.Run(fmt.Sprintf("inspect=%t", inspect), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			listener, err := (minecraft.ListenConfig{AuthenticationDisabled: true, ErrorLog: logger}).Listen("raknet", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			edge, backend := net.Pipe()
			defer edge.Close()
			defer backend.Close()
			_ = backend.SetDeadline(time.Now().Add(15 * time.Second))
			processor := &loginShieldProcessor{
				early:     make(chan struct{}),
				registry:  make(chan int16, 1),
				equipment: make(chan packet.MobEquipment, 1),
			}
			backendReady := make(chan error, 1)
			backendDone := make(chan struct{})
			go func() {
				defer close(backendDone)
				err := loginShieldBackend(ctx, backend, processor.early)
				backendReady <- err
				if err == nil {
					_, _ = io.Copy(io.Discard, backend)
				}
			}()
			sessions := make(chan *Session, 1)
			loginResult := make(chan error, 1)
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					loginResult <- err
					return
				}
				opts := *util.DefaultOpts()
				opts.LatencyInterval = time.Hour.Milliseconds()
				opts.ClientDecode = []uint32{packet.IDText}
				if inspect {
					opts.ClientTrace = []uint32{packet.IDMobEquipment}
				} else {
					opts.ClientDecode = append(opts.ClientDecode, packet.IDMobEquipment)
				}
				s := NewSession(conn.(*minecraft.Conn), logger, NewRegistry(), loginShieldDiscovery{}, opts, testTransport{
					dial: func(context.Context, string) (io.ReadWriteCloser, error) { return edge, nil },
				})
				s.SetProcessor(processor)
				sessions <- s
				loginResult <- s.LoginContext(ctx)
			}()
			// The native dialer returns only after spawn. Capture its connection
			// through the ordinary protocol hook so we can send a packet after
			// authentication but before the backend is allowed to start the game.
			clientProtocol := &loginShieldClientProtocol{Protocol: minecraft.DefaultProtocol, conn: make(chan *minecraft.Conn, 1)}
			dialResult := make(chan error, 1)
			go func() {
				_, err := (minecraft.Dialer{ErrorLog: logger, Protocol: clientProtocol}).DialContext(ctx, "raknet", listener.Addr().String())
				dialResult <- err
			}()
			var client *minecraft.Conn
			select {
			case client = <-clientProtocol.conn:
				defer client.Close()
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			var s *Session
			select {
			case s = <-sessions:
				defer s.Close()
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}

			// The backend refuses to publish any GameData until this packet has
			// reached the early client pump. Moving that pump after StartGame or
			// caching the shield ID before it starts is therefore caught reliably.
			if err := client.WritePacket(&packet.Text{TextType: packet.TextTypeRaw, Message: "early-pump"}); err != nil {
				t.Fatal(err)
			}
			if err := client.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := client.DoSpawnContext(ctx); err != nil {
				t.Fatalf("public spawn with early packet pump: %v", err)
			}
			for _, result := range []<-chan error{dialResult, loginResult, backendReady} {
				select {
				case err := <-result:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			if got := <-processor.registry; got != backendLoginShieldID {
				t.Fatalf("processor input shield = %d, want backend ID %d", got, backendLoginShieldID)
			}

			want := packet.MobEquipment{EntityRuntimeID: 1, NewItem: protocol.ItemInstance{
				StackNetworkID: 1,
				Stack:          protocol.ItemStack{ItemType: protocol.ItemType{NetworkID: int32(processedLoginShieldID)}, Count: 1, BlockingTick: 1234567890123},
			}}
			if err := client.WritePacket(&want); err != nil {
				t.Fatal(err)
			}
			if err := client.Flush(); err != nil {
				t.Fatal(err)
			}
			select {
			case got := <-processor.equipment:
				if got.NewItem.Stack.NetworkID != want.NewItem.Stack.NetworkID || got.NewItem.Stack.BlockingTick != want.NewItem.Stack.BlockingTick {
					t.Fatalf("decoded shield = %+v, want %+v", got.NewItem.Stack, want.NewItem.Stack)
				}
			case <-ctx.Done():
				t.Fatal("client shield packet was not decoded: ", ctx.Err())
			}
			_ = s.Close()
			select {
			case <-backendDone:
			case <-ctx.Done():
				t.Fatal("backend did not stop: ", ctx.Err())
			}
		})
	}
}

const (
	backendLoginShieldID   int16 = 511
	processedLoginShieldID int16 = 812
)

type loginShieldProcessor struct {
	NopProcessor
	earlyOnce sync.Once
	early     chan struct{}
	registry  chan int16
	equipment chan packet.MobEquipment
}

func (p *loginShieldProcessor) ProcessStartGame(_ *Context, data *minecraft.GameData) {
	data.Items = slices.Clone(data.Items)
	for i, item := range data.Items {
		if item.Name == "minecraft:shield" {
			p.registry <- item.RuntimeID
			data.Items[i].RuntimeID = processedLoginShieldID
			return
		}
	}
	p.registry <- 0
}

func (p *loginShieldProcessor) ProcessClient(ctx *Context, pk *packet.Packet) {
	if text, ok := (*pk).(*packet.Text); ok && text.Message == "early-pump" {
		p.earlyOnce.Do(func() { close(p.early) })
		ctx.Cancel()
	}
	p.ProcessClientInspect(ctx, *pk)
}

func (p *loginShieldProcessor) ProcessClientInspect(ctx *Context, pk packet.Packet) {
	if equipment, ok := pk.(*packet.MobEquipment); ok {
		p.equipment <- *equipment
		ctx.Cancel()
	}
}

type loginShieldDiscovery struct{}

func (loginShieldDiscovery) Discover(*minecraft.Conn) (string, error) { return "memory", nil }
func (loginShieldDiscovery) DiscoverFallback(*minecraft.Conn) (string, error) {
	return "", errors.New("no fallback in login regression")
}

type loginShieldClientProtocol struct {
	minecraft.Protocol
	once sync.Once
	conn chan *minecraft.Conn
}

func (p *loginShieldClientProtocol) ConvertFromLatest(pk packet.Packet, conn *minecraft.Conn) []packet.Packet {
	p.once.Do(func() { p.conn <- conn })
	return p.Protocol.ConvertFromLatest(pk, conn)
}

func loginShieldBackend(ctx context.Context, backend net.Conn, early <-chan struct{}) error {
	reader, writer := framing.NewReader(backend), framing.NewWriter(backend)
	expect := func(want uint32) error {
		encoded, err := reader.ReadPacket()
		if err != nil {
			return err
		}
		payload, err := snappy.Decode(nil, encoded)
		if err != nil {
			return err
		}
		header := new(packet.Header)
		if err := header.Read(bytes.NewBuffer(payload)); err != nil {
			return err
		}
		if header.PacketID != want {
			return fmt.Errorf("backend packet = %d, want %d", header.PacketID, want)
		}
		return nil
	}
	write := func(pk packet.Packet) error {
		var payload bytes.Buffer
		if err := (&packet.Header{PacketID: pk.ID()}).Write(&payload); err != nil {
			return err
		}
		pk.Marshal(minecraft.DefaultProtocol.NewWriter(&payload, int32(backendLoginShieldID)))
		// The decoded marker belongs only to the backend-to-Spectrum direction.
		return writer.Write(append([]byte{0}, snappy.Encode(nil, payload.Bytes())...))
	}
	if err := expect(spectrumpacket.IDConnectionRequest); err != nil {
		return err
	}
	select {
	case <-early:
	case <-ctx.Done():
		return ctx.Err()
	}
	for _, pk := range []packet.Packet{
		&spectrumpacket.ConnectionResponse{RuntimeID: 1, UniqueID: 1},
		&packet.StartGame{WorldName: "login-shield-regression", EntityRuntimeID: 1, EntityUniqueID: 1, BaseGameVersion: protocol.CurrentVersion},
		&packet.ItemRegistry{Items: []protocol.ItemEntry{{Name: "minecraft:shield", RuntimeID: backendLoginShieldID}}},
	} {
		if err := write(pk); err != nil {
			return err
		}
	}
	if err := expect(packet.IDRequestChunkRadius); err != nil {
		return err
	}
	if err := write(&packet.ChunkRadiusUpdated{ChunkRadius: 8}); err != nil {
		return err
	}
	if err := write(&packet.PlayStatus{Status: packet.PlayStatusPlayerSpawn}); err != nil {
		return err
	}
	return expect(packet.IDSetLocalPlayerAsInitialised)
}
