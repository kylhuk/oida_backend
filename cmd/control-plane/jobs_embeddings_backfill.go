package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"global-osint-backend/internal/embeddings"
	"global-osint-backend/internal/migrate"
)

const (
	embeddingsBackfillJobName = "embeddings-backfill"
	embeddingsBatchSize       = 64
)

func init() {
	jobRegistry[embeddingsBackfillJobName] = jobRunner{
		description: "Generate and store text embeddings for all entities. Idempotent on source_text_sha256.",
		run:         runEmbeddingsBackfill,
	}
}

func embeddingServiceURL() string {
	if v := strings.TrimSpace(os.Getenv("EMBEDDING_SERVICE_URL")); v != "" {
		return v
	}
	return "http://embed:8080"
}

// entityRow holds the fields we need to build canonical text for embedding.
type entityRow struct {
	EntityID   string `json:"entity_id"`
	EntityType string `json:"entity_type"`
	Name       string `json:"canonical_name"`
	Attrs      string `json:"attrs"`
}

func runEmbeddingsBackfill(ctx context.Context) error {
	runner := migrate.NewHTTPRunner(controlPlaneClickHouseURL())
	client := embeddings.New(embeddingServiceURL())
	startedAt := time.Now().UTC()
	jobID := fmt.Sprintf("job:%s:%d", embeddingsBackfillJobName, startedAt.UnixMilli())

	// Load all registered vector spaces.
	spacesOut, err := runner.Query(ctx,
		"SELECT name, version, dimensions FROM meta.vector_space FINAL WHERE enabled = 1 FORMAT JSONEachRow")
	if err != nil {
		return recordOpsJobFailure(ctx, runner, jobID, embeddingsBackfillJobName, startedAt,
			"failed to load vector spaces", err, nil)
	}

	type spaceRow struct {
		Name       string `json:"name"`
		Version    string `json:"version"`
		Dimensions uint32 `json:"dimensions"`
	}
	var spaces []spaceRow
	for _, line := range strings.Split(strings.TrimSpace(spacesOut), "\n") {
		if line == "" {
			continue
		}
		var s spaceRow
		if err := json.Unmarshal([]byte(line), &s); err == nil {
			spaces = append(spaces, s)
		}
	}
	if len(spaces) == 0 {
		return recordJobRun(ctx, runner, jobID, embeddingsBackfillJobName, "success",
			startedAt, time.Now().UTC().Truncate(time.Millisecond),
			"no enabled vector spaces found; nothing to embed", map[string]any{"embedded_count": 0})
	}

	// Load all entities that need embedding.
	entitiesOut, err := runner.Query(ctx,
		"SELECT entity_id, entity_type, canonical_name, attrs "+
			"FROM gold.api_v1_entities FORMAT JSONEachRow")
	if err != nil {
		return recordOpsJobFailure(ctx, runner, jobID, embeddingsBackfillJobName, startedAt,
			"failed to load entities", err, nil)
	}

	var entities []entityRow
	for _, line := range strings.Split(strings.TrimSpace(entitiesOut), "\n") {
		if line == "" {
			continue
		}
		var e entityRow
		if err := json.Unmarshal([]byte(line), &e); err == nil {
			entities = append(entities, e)
		}
	}

	totalEmbedded := 0

	for _, space := range spaces {
		// Load already-embedded sha256 fingerprints for this space/version to
		// skip rows that haven't changed.
		existingOut, _ := runner.Query(ctx, fmt.Sprintf(
			"SELECT entity_id, source_text_sha256 "+
				"FROM silver.entity_embedding FINAL "+
				"WHERE vector_space = '%s' AND version = '%s' FORMAT JSONEachRow",
			space.Name, space.Version))
		existing := make(map[string]string)
		for _, line := range strings.Split(strings.TrimSpace(existingOut), "\n") {
			if line == "" {
				continue
			}
			var r struct {
				EntityID string `json:"entity_id"`
				SHA256   string `json:"source_text_sha256"`
			}
			if err := json.Unmarshal([]byte(line), &r); err == nil {
				existing[r.EntityID] = r.SHA256
			}
		}

		// Determine which entities need (re-)embedding.
		var pending []entityRow
		for _, e := range entities {
			text := canonicalText(e)
			hash := sha256Hex(text)
			if existing[e.EntityID] == hash {
				continue // unchanged
			}
			pending = append(pending, e)
		}

		// Embed in batches.
		for i := 0; i < len(pending); i += embeddingsBatchSize {
			end := i + embeddingsBatchSize
			if end > len(pending) {
				end = len(pending)
			}
			batch := pending[i:end]

			texts := make([]string, len(batch))
			for j, e := range batch {
				texts[j] = canonicalText(e)
			}

			vecs, err := client.Embed(ctx, texts)
			if err != nil {
				return recordOpsJobFailure(ctx, runner, jobID, embeddingsBackfillJobName, startedAt,
					"embedding service error", err, map[string]any{"embedded_so_far": totalEmbedded})
			}

			// Build INSERT rows.
			var rows []string
			now := time.Now().UTC().Format("2006-01-02 15:04:05.000")
			for j, e := range batch {
				hash := sha256Hex(texts[j])
				vecJSON := float32SliceToJSON(vecs[j])
				rows = append(rows, fmt.Sprintf(
					"('%s','%s','%s','%s',%s,'%s','%s',1,1,1,'%s','{}','[]')",
					space.Name, space.Version, e.EntityID, e.EntityType,
					vecJSON, hash, now, now,
				))
			}

			insertSQL := "INSERT INTO silver.entity_embedding " +
				"(vector_space, version, entity_id, entity_type, embedding, " +
				"source_text_sha256, generated_at, schema_version, record_version, " +
				"api_contract_version, updated_at, attrs, evidence) VALUES " +
				strings.Join(rows, ",")

			if err := runner.ApplySQL(ctx, insertSQL); err != nil {
				return recordOpsJobFailure(ctx, runner, jobID, embeddingsBackfillJobName, startedAt,
					"insert failed", err, map[string]any{"embedded_so_far": totalEmbedded})
			}
			totalEmbedded += len(batch)
		}
	}

	return recordJobRun(ctx, runner, jobID, embeddingsBackfillJobName, "success",
		startedAt, time.Now().UTC().Truncate(time.Millisecond),
		"embeddings backfill completed", map[string]any{"embedded_count": totalEmbedded})
}

// canonicalText builds the text we embed for an entity.
// Uses canonical_name as the primary signal; appends entity_type for disambiguation.
func canonicalText(e entityRow) string {
	name := strings.TrimSpace(e.Name)
	if name == "" {
		name = e.EntityID
	}
	return fmt.Sprintf("%s [%s]", name, e.EntityType)
}

func sha256Hex(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func float32SliceToJSON(v []float32) string {
	if len(v) == 0 {
		return "[]"
	}
	var b strings.Builder
	b.WriteString("[")
	for i, f := range v {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "%g", f)
	}
	b.WriteString("]")
	return b.String()
}
