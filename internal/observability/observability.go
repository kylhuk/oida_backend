package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"
)

const RequestIDHeader = "X-Request-ID"

type correlationIDContextKey struct{}

func WithCorrelationID(ctx context.Context, correlationID string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if normalized := NormalizeCorrelationID(correlationID); normalized != "" {
		return context.WithValue(ctx, correlationIDContextKey{}, normalized)
	}
	return ctx
}

func CorrelationID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	value, _ := ctx.Value(correlationIDContextKey{}).(string)
	return NormalizeCorrelationID(value)
}

func CorrelationIDFromRequest(r *http.Request, prefix string) string {
	if r != nil {
		if existing := NormalizeCorrelationID(r.Header.Get(RequestIDHeader)); existing != "" {
			return existing
		}
		if existing := CorrelationID(r.Context()); existing != "" {
			return existing
		}
	}
	return NewCorrelationID(prefix)
}

func NewCorrelationID(prefix string) string {
	normalizedPrefix := NormalizeCorrelationID(prefix)
	if normalizedPrefix == "" {
		normalizedPrefix = "trace"
	}
	randomBytes := make([]byte, 8)
	if _, err := rand.Read(randomBytes); err != nil {
		return normalizedPrefix + ":fallback"
	}
	return normalizedPrefix + ":" + hex.EncodeToString(randomBytes)
}

func NormalizeCorrelationID(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(raw))
	for _, r := range raw {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case strings.ContainsRune("-._:/", r):
			b.WriteRune(r)
		}
		if b.Len() >= 128 {
			break
		}
	}
	return strings.TrimSpace(b.String())
}

func LogEvent(component, event, correlationID string, fields map[string]any) {
	payload := map[string]any{
		"ts":             time.Now().UTC().Format(time.RFC3339Nano),
		"component":      strings.TrimSpace(component),
		"event":          strings.TrimSpace(event),
		"correlation_id": NormalizeCorrelationID(correlationID),
	}
	for key, value := range fields {
		trimmed := strings.TrimSpace(key)
		if trimmed == "" {
			continue
		}
		payload[trimmed] = value
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		log.Printf("{\"component\":%q,\"event\":%q,\"correlation_id\":%q,\"marshal_error\":%q}", component, event, NormalizeCorrelationID(correlationID), err.Error())
		return
	}
	log.Print(string(encoded))
}
