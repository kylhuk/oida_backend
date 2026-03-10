package canonical

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

type IDStrategy string

const (
	IDStrategySourceNative IDStrategy = "source_native"
	IDStrategyContentHash  IDStrategy = "content_hash"
)

type Identity struct {
	ID       string     `json:"id"`
	Strategy IDStrategy `json:"id_strategy"`
}

type IDOptions struct {
	Namespace string
	SourceID  string
	NativeID  string
	Content   any
}

var safeIDComponentPattern = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

func NewIdentity(opts IDOptions) Identity {
	namespace := normalizeNamespace(opts.Namespace)
	source := normalizeIDComponent(opts.SourceID)

	nativeID := strings.TrimSpace(opts.NativeID)
	if nativeID != "" {
		return Identity{
			ID:       strings.Join([]string{namespace, source, normalizeIDComponent(nativeID)}, ":"),
			Strategy: IDStrategySourceNative,
		}
	}

	hash := HashContent(map[string]any{
		"namespace": namespace,
		"source_id": strings.TrimSpace(opts.SourceID),
		"content":   opts.Content,
	})
	return Identity{
		ID:       strings.Join([]string{namespace, source, hash}, ":"),
		Strategy: IDStrategyContentHash,
	}
}

func HashContent(v any) string {
	encoded, err := json.Marshal(v)
	if err != nil {
		encoded = []byte(fmt.Sprintf("%#v", v))
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func normalizeNamespace(v string) string {
	v = normalizeIDComponent(v)
	if v == "unknown" {
		return "record"
	}
	return v
}

func normalizeIDComponent(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return "unknown"
	}
	v = safeIDComponentPattern.ReplaceAllString(v, "_")
	v = strings.Trim(v, "._-")
	if v == "" {
		return "unknown"
	}
	for strings.Contains(v, "__") {
		v = strings.ReplaceAll(v, "__", "_")
	}
	if len(v) > 96 {
		return "sha256-" + HashContent(v)
	}
	return v
}
