package frontend

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestSignalingLogOmitsOffer(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(offerRedactor{slog.NewJSONHandler(&buf, nil)})
	logger.With("offer", "private-identity").WithGroup("signaling").Error("negotiation failed", "offer", "private-token", "error", "invalid offer")
	if strings.Contains(buf.String(), "private-") || !strings.Contains(buf.String(), "invalid offer") {
		t.Fatal("signaling log did not redact credentials while retaining error")
	}
}
