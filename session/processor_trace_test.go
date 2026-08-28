package session

import (
	"testing"
	"time"
)

var _ TraceFlushProcessor = NopProcessor{}

func TestTraceContextRequestsAreIndependent(t *testing.T) {
	ctx := NewContext()
	ctx.RequestTrace()
	ctx.RequestTraceAcknowledgement()
	ctx.RequestFlush()
	ctx.RequestFollowupFlushes(0, -time.Millisecond, 10*time.Millisecond, 25*time.Millisecond)
	if !ctx.trace() || !ctx.traceAck() || !ctx.flush() || ctx.Cancelled() {
		t.Fatalf("unexpected context state: %#v", ctx)
	}
	followups := ctx.followups()
	if len(followups) != 2 || followups[0] != 10*time.Millisecond || followups[1] != 25*time.Millisecond {
		t.Fatalf("unexpected follow-up flushes: %v", followups)
	}
	ctx.Cancel()
	if !ctx.Cancelled() || !ctx.trace() || !ctx.traceAck() || !ctx.flush() {
		t.Fatalf("cancel changed trace requests: %#v", ctx)
	}
}
