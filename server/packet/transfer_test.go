package packet

import (
	"bytes"
	"testing"

	"github.com/sandertv/gophertunnel/minecraft/protocol"
)

func TestTransferRoundTripPreservesReadyBarrier(t *testing.T) {
	want := &Transfer{Addr: "replay:19144", WaitForReady: true}
	var buffer bytes.Buffer
	want.Marshal(protocol.NewWriter(&buffer, 0))

	got := &Transfer{}
	got.Marshal(protocol.NewReader(&buffer, 0, false))
	if got.Addr != want.Addr || got.WaitForReady != want.WaitForReady {
		t.Fatalf("decoded transfer = %#v, want %#v", got, want)
	}
}
