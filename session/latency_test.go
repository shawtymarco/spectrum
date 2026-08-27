package session

import "testing"

func TestReportedLatencyDoesNotDoubleCountClientLeg(t *testing.T) {
	if got, want := reportedLatency(20, 70), int64(70); got != want {
		t.Fatalf("reported latency = %dms, want %dms", got, want)
	}
}

func TestReportedLatencyFallsBackToRakNetRTT(t *testing.T) {
	if got, want := reportedLatency(20, 0), int64(40); got != want {
		t.Fatalf("fallback latency = %dms, want %dms", got, want)
	}
}
