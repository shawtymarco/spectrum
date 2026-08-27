package packet

import "github.com/sandertv/gophertunnel/minecraft/protocol"

// Transfer is sent by the server to initiate a server transfer.
type Transfer struct {
	// Addr is the address of the new server.
	Addr string
	// WaitForReady keeps the client in the transfer animation until the new
	// backend publishes BackendReady and its first final-world chunk.
	WaitForReady bool
}

// ID ...
func (pk *Transfer) ID() uint32 {
	return IDTransfer
}

// Marshal ...
func (pk *Transfer) Marshal(io protocol.IO) {
	io.String(&pk.Addr)
	io.Bool(&pk.WaitForReady)
}
