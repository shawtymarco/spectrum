package session

import "testing"

func TestTraceContextRequestsAreIndependent(t *testing.T) {
	ctx := NewContext()
	ctx.RequestTrace()
	ctx.RequestTraceAcknowledgement()
	ctx.RequestFlush()
	if !ctx.trace() || !ctx.traceAck() || !ctx.flush() || ctx.Cancelled() {
		t.Fatalf("unexpected context state: %#v", ctx)
	}
	ctx.Cancel()
	if !ctx.Cancelled() || !ctx.trace() || !ctx.traceAck() || !ctx.flush() {
		t.Fatalf("cancel changed trace requests: %#v", ctx)
	}
}
