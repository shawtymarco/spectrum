package session

import (
	"time"

	spectrumpacket "github.com/cooldogedev/spectrum/server/packet"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

// Context represents the context of an action. It holds the state of whether the action has been canceled.
type Context struct {
	canceled          bool
	traceRequested    bool
	flushRequested    bool
	traceAckRequested bool
}

// NewContext returns a new context.
func NewContext() *Context {
	return &Context{}
}

// Cancel marks the context as canceled. This function is used to stop further processing of an action.
func (c *Context) Cancel() {
	c.canceled = true
}

// Cancelled returns whether the context has been cancelled.
func (c *Context) Cancelled() bool {
	return c.canceled
}

// RequestTrace asks Spectrum to carry the current packet in an internal trace
// envelope while preserving its Bedrock payload and stream order.
func (c *Context) RequestTrace() { c.traceRequested = true }

func (c *Context) trace() bool { return c.traceRequested }

// RequestFlush asks Spectrum to flush the public client's existing ordered
// packet prefix after processing the current internal marker.
func (c *Context) RequestFlush() { c.flushRequested = true }

func (c *Context) flush() bool { return c.flushRequested }

// RequestTraceAcknowledgement asks Spectrum to append a NetworkStackLatency
// acknowledgement probe before any requested flush.
func (c *Context) RequestTraceAcknowledgement() { c.traceAckRequested = true }

func (c *Context) traceAck() bool { return c.traceAckRequested }

// TraceStart reports that a traced client packet was written to the backend.
type TraceStart struct {
	ID                   uint64
	ReceivedAt           time.Time
	BackendWriteDuration time.Duration
}

// TraceAck reports a matching client response to a trace acknowledgement
// probe. Duration starts when the probe was queued behind feedback.
type TraceAck struct {
	ID       uint64
	Role     uint8
	Duration time.Duration
}

// Processor defines methods for processing various actions within a proxy session.
type Processor interface {
	// ProcessStartGame is called only once during the login sequence.
	ProcessStartGame(ctx *Context, data *minecraft.GameData)
	// ProcessServer is called before forwarding the server-sent packets to the client.
	ProcessServer(ctx *Context, pk *packet.Packet)
	// ProcessServerEncoded is called before forwarding the server-sent packets to the client.
	ProcessServerEncoded(ctx *Context, pk *[]byte)
	// ProcessClient is called before forwarding the client-sent packets to the server.
	ProcessClient(ctx *Context, pk *packet.Packet)
	// ProcessClientInspect sees a decoded copy of a packet that otherwise keeps
	// Spectrum's native raw forwarding path. Packet mutations are ignored.
	ProcessClientInspect(ctx *Context, pk packet.Packet)
	// ProcessClientEncoded is called before forwarding the client-sent packets to the server.
	ProcessClientEncoded(ctx *Context, pk *[]byte)
	ProcessTraceStart(ctx *Context, trace TraceStart)
	ProcessTraceResult(ctx *Context, result *spectrumpacket.TraceResult)
	ProcessTraceAck(ctx *Context, ack TraceAck)
	// ProcessFlush is called before flushing the player's minecraft.Conn buffer in response to a downstream server request.
	ProcessFlush(ctx *Context)
	// ProcessPreTransfer is called before transferring the player to a different server.
	ProcessPreTransfer(ctx *Context, origin *string, target *string)
	// ProcessTransferFailure is called when the player transfer to a different server fails.
	ProcessTransferFailure(ctx *Context, origin *string, target *string, err error)
	// ProcessPostTransfer is called after transferring the player to a different server.
	ProcessPostTransfer(ctx *Context, origin *string, target *string)
	// ProcessCache is called before updating the session's cache.
	ProcessCache(ctx *Context, new *[]byte)
	// ProcessDisconnection is called when the player disconnects from the proxy.
	ProcessDisconnection(ctx *Context, message *string)
	// ProcessPreFallback is called before transferring the player to a fallback server.
	ProcessPreFallback(ctx *Context, origin *string, target *string)
	// ProcessFallbackFailure is called when the fallback transfer fails.
	ProcessFallbackFailure(ctx *Context, origin *string, target *string, err error)
	// ProcessPostFallback is called after transferring the player to a fallback server.
	ProcessPostFallback(ctx *Context, origin *string, target *string)
}

// NopProcessor is a no-operation implementation of the Processor interface.
type NopProcessor struct{}

// Ensure that NopProcessor satisfies the Processor interface.
var _ Processor = NopProcessor{}

func (NopProcessor) ProcessStartGame(_ *Context, _ *minecraft.GameData)               {}
func (NopProcessor) ProcessServer(_ *Context, _ *packet.Packet)                       {}
func (NopProcessor) ProcessServerEncoded(_ *Context, _ *[]byte)                       {}
func (NopProcessor) ProcessClient(_ *Context, _ *packet.Packet)                       {}
func (NopProcessor) ProcessClientInspect(_ *Context, _ packet.Packet)                 {}
func (NopProcessor) ProcessClientEncoded(_ *Context, _ *[]byte)                       {}
func (NopProcessor) ProcessTraceStart(_ *Context, _ TraceStart)                       {}
func (NopProcessor) ProcessTraceResult(_ *Context, _ *spectrumpacket.TraceResult)     {}
func (NopProcessor) ProcessTraceAck(_ *Context, _ TraceAck)                           {}
func (NopProcessor) ProcessFlush(_ *Context)                                          {}
func (NopProcessor) ProcessPreTransfer(_ *Context, _ *string, _ *string)              {}
func (NopProcessor) ProcessTransferFailure(_ *Context, _ *string, _ *string, _ error) {}
func (NopProcessor) ProcessPostTransfer(_ *Context, _ *string, _ *string)             {}
func (NopProcessor) ProcessCache(_ *Context, _ *[]byte)                               {}
func (NopProcessor) ProcessDisconnection(_ *Context, _ *string)                       {}
func (NopProcessor) ProcessPreFallback(_ *Context, _ *string, _ *string)              {}
func (NopProcessor) ProcessFallbackFailure(_ *Context, _ *string, _ *string, _ error) {}
func (NopProcessor) ProcessPostFallback(_ *Context, _ *string, _ *string)             {}
