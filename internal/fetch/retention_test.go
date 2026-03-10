package fetch

import (
	"context"
	"testing"
	"time"
)

func TestRetentionReplayClasses(t *testing.T) {
	fetchedAt := time.Date(2026, time.March, 10, 12, 0, 0, 0, time.UTC)
	req := Request{
		Method: "GET",
		URL:    "https://example.com/feed.json",
		Source: SourcePolicy{SourceID: "fixture:site", RetentionClass: "warm", SupportsLiveGET: true},
	}

	t.Run("small bodies stay inline and replay from metadata", func(t *testing.T) {
		store := &stubObjectStore{}
		resp := Response{
			FetchURL:    req.URL,
			FinalURL:    req.URL,
			SourceID:    req.Source.SourceID,
			Method:      "GET",
			StatusCode:  200,
			Success:     true,
			FetchedAt:   fetchedAt,
			Body:        []byte(`{"hello":"world"}`),
			BodyBytes:   int64(len([]byte(`{"hello":"world"}`))),
			ContentHash: sha256Hex([]byte(`{"hello":"world"}`)),
			ContentType: "application/json",
		}

		persisted, err := RetainResponse(context.Background(), PersistOptions{
			FetchID:  "fetch:inline",
			RawID:    "raw:inline",
			SourceID: req.Source.SourceID,
			Bucket:   "raw",
			Policy:   ResolveRetentionPolicy("warm"),
			Now:      fetchedAt,
		}, req, resp, store)
		if err != nil {
			t.Fatalf("retain response: %v", err)
		}
		if persisted.RawDocument == nil {
			t.Fatal("expected raw document metadata to be written")
		}
		if persisted.RawDocument.ObjectKey != nil {
			t.Fatalf("expected inline body, got object key %q", *persisted.RawDocument.ObjectKey)
		}
		if len(store.objects) != 0 {
			t.Fatalf("expected no object-store writes, got %d", len(store.objects))
		}
		replayed, err := Replay(context.Background(), *persisted.RawDocument, store)
		if err != nil {
			t.Fatalf("replay inline document: %v", err)
		}
		if string(replayed.Body) != `{"hello":"world"}` {
			t.Fatalf("unexpected inline replay body %q", replayed.Body)
		}
		if replayed.Metadata.ReplaySource != "inline" {
			t.Fatalf("expected inline replay source, got %q", replayed.Metadata.ReplaySource)
		}
	})

	t.Run("large bodies spill to object storage and replay from bucket", func(t *testing.T) {
		store := &stubObjectStore{}
		largeBody := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
		policy := ResolveRetentionPolicy("cold")
		policy.InlineBodyMaxBytes = 8
		resp := Response{
			FetchURL:    req.URL,
			FinalURL:    req.URL,
			SourceID:    req.Source.SourceID,
			Method:      "GET",
			StatusCode:  200,
			Success:     true,
			FetchedAt:   fetchedAt,
			Body:        largeBody,
			BodyBytes:   int64(len(largeBody)),
			ContentHash: sha256Hex(largeBody),
			ContentType: "text/plain",
		}

		persisted, err := RetainResponse(context.Background(), PersistOptions{
			FetchID:  "fetch:object",
			RawID:    "raw:object",
			SourceID: req.Source.SourceID,
			Bucket:   "raw",
			Policy:   policy,
			Now:      fetchedAt,
		}, req, resp, store)
		if err != nil {
			t.Fatalf("retain response: %v", err)
		}
		if persisted.RawDocument == nil || persisted.RawDocument.ObjectKey == nil {
			t.Fatal("expected large body to be written to object storage")
		}
		if len(store.objects) != 1 {
			t.Fatalf("expected one object write, got %d", len(store.objects))
		}
		replayed, err := Replay(context.Background(), *persisted.RawDocument, store)
		if err != nil {
			t.Fatalf("replay object document: %v", err)
		}
		if string(replayed.Body) != string(largeBody) {
			t.Fatalf("unexpected replay body %q", replayed.Body)
		}
		if replayed.Metadata.ReplaySource != "object-store" {
			t.Fatalf("expected object-store replay source, got %q", replayed.Metadata.ReplaySource)
		}
	})

	t.Run("policy mapping stays deterministic", func(t *testing.T) {
		if got := ResolveRetentionPolicy("archive"); got.ReplayClass != ReplayClassReplayOnly || !got.ForceObjectStore {
			t.Fatalf("unexpected archive policy: %#v", got)
		}
		if got := ResolveRetentionPolicy("warm"); got.InlineBodyMaxBytes == 0 || got.ReplayClass != ReplayClassCached {
			t.Fatalf("unexpected warm policy: %#v", got)
		}
	})
}

type stubObjectStore struct {
	objects map[string]stubObject
}

type stubObject struct {
	body        []byte
	contentType string
}

func (s *stubObjectStore) PutObject(_ context.Context, bucket, key string, body []byte, contentType string) error {
	if s.objects == nil {
		s.objects = make(map[string]stubObject)
	}
	compoundKey := bucket + "/" + key
	s.objects[compoundKey] = stubObject{body: append([]byte(nil), body...), contentType: contentType}
	return nil
}

func (s *stubObjectStore) GetObject(_ context.Context, bucket, key string) ([]byte, string, error) {
	object := s.objects[bucket+"/"+key]
	return append([]byte(nil), object.body...), object.contentType, nil
}
