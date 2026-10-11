package frontend

import (
	"context"
	"log/slog"
)

type offerRedactor struct{ slog.Handler }

func (h offerRedactor) Handle(ctx context.Context, r slog.Record) error {
	clean := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	r.Attrs(func(a slog.Attr) bool {
		if a.Key != "offer" {
			clean.AddAttrs(a)
		}
		return true
	})
	return h.Handler.Handle(ctx, clean)
}
func (h offerRedactor) WithAttrs(attrs []slog.Attr) slog.Handler {
	clean := make([]slog.Attr, 0, len(attrs))
	for _, a := range attrs {
		if a.Key != "offer" {
			clean = append(clean, a)
		}
	}
	return offerRedactor{h.Handler.WithAttrs(clean)}
}
func (h offerRedactor) WithGroup(name string) slog.Handler {
	return offerRedactor{h.Handler.WithGroup(name)}
}
