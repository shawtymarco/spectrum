package util

// Opts defines the configuration options for Spectrum.
type Opts struct {
	// Addr is the address to listen on.
	Addr string `yaml:"addr"`
	// AutoLogin determines whether automatic login should be enabled.
	AutoLogin bool `yaml:"auto_login"`
	// ClientDecode is a list of client packet identifiers that need to be decoded by the proxy.
	ClientDecode []uint32 `yaml:"client_decode"`
	// ClientTrace is a list of native client packet identifiers inspected from
	// a copy while their original encoded payload retains the raw fast path.
	// The processor may request an internal trace for selected packet values.
	ClientTrace []uint32 `yaml:"client_trace"`
	// ClientDiscard is a list of client packet identifiers discarded after the
	// header is read but before their payload is decoded or forwarded.
	ClientDiscard []uint32 `yaml:"client_discard"`
	// LatencyInterval is the interval at which the latency of the connection is updated in milliseconds.
	// Lower intervals provide more accurate latency but use more bandwidth.
	LatencyInterval int64 `yaml:"latency_interval"`
	// ShutdownMessage is the message displayed to clients when Spectrum shuts down.
	ShutdownMessage string `yaml:"shutdown_message"`
	// SyncProtocol determines the protocol version the proxy should use when communicating with servers.
	// When enabled, the proxy uses the client's protocol version (minecraft.Protocol) for reading and
	// writing packets. If disabled, the proxy defaults to using the latest protocol version (minecraft.DefaultProtocol).
	SyncProtocol bool `yaml:"sync_protocol"`
}

// DefaultOpts returns the default configuration options for Spectrum.
func DefaultOpts() *Opts {
	return &Opts{
		Addr:            ":19132",
		AutoLogin:       true,
		LatencyInterval: 3000,
		ShutdownMessage: "Spectrum closed.",
		SyncProtocol:    false,
	}
}
