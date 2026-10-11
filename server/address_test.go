package server

import (
	"github.com/df-mc/go-nethernet"
	"net"
	"testing"
)

func TestConnectionRequestPublicAddress(t *testing.T) {
	n := &nethernet.Addr{NetworkID: "123", ConnectionID: 456}
	if got := publicAddress(n); got != "nethernet://123/456" {
		t.Fatalf("opaque address %q", got)
	}
	a := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 19132}
	if got := publicAddress(a); got != "127.0.0.1:19132" {
		t.Fatalf("RakNet address changed: %s", got)
	}
}
