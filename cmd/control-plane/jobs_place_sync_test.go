package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestRunGeoBoundariesSyncStagesArtifactAndRecordsJobRun(t *testing.T) {
	var sourceHits int
	sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceHits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"dataset":"gbOpen","scope":"ALL"}`)
	}))
	defer sourceServer.Close()

	minio := newRecordingObjectStoreServer()
	defer minio.Close()
	clickhouse := newRecordingClickHouseServer()
	defer clickhouse.Close()

	t.Setenv("CLICKHOUSE_HTTP_URL", clickhouse.URL)
	t.Setenv("MINIO_ENDPOINT", minio.URL)
	t.Setenv("MINIO_ACCESS_KEY", "minioadmin")
	t.Setenv("MINIO_SECRET_KEY", "minioadmin")
	t.Setenv("STAGE_BUCKET", "stage")
	t.Setenv("PLACE_STAGE_PREFIX", "place-datasets")
	t.Setenv("GEOBOUNDARIES_GBOPEN_URL", sourceServer.URL+"/gbopen.json")

	if err := runGeoBoundariesSync(context.Background()); err != nil {
		t.Fatalf("runGeoBoundariesSync: %v", err)
	}
	if sourceHits != 1 {
		t.Fatalf("expected one source download, got %d", sourceHits)
	}
	if len(minio.puts) != 1 {
		t.Fatalf("expected one staged object, got %#v", minio.puts)
	}
	put := minio.puts[0]
	if put.Path != "/stage/place-datasets/geoboundaries-sync/current/gbOpen_ALL_ALL.json" {
		t.Fatalf("unexpected staged path: %#v", put)
	}
	if strings.TrimSpace(put.ContentType) != "application/json" {
		t.Fatalf("unexpected staged content type: %#v", put)
	}
	joined := strings.Join(clickhouse.queries, "\n")
	for _, want := range []string{"INSERT INTO ops.job_run", "geoboundaries-sync", "staged geoBoundaries gbOpen metadata", "gbOpen_ALL_ALL.json", sourceServer.URL + "/gbopen.json"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected ClickHouse query to contain %q, got %s", want, joined)
		}
	}
}

func TestRunGeoNamesSyncStagesArtifactsAndSupportsReruns(t *testing.T) {
	responses := map[string]string{
		"/countryInfo.txt":      "FR\tFrance\nUS\tUnited States\n",
		"/admin1CodesASCII.txt": "FR.A8\tIle-de-France\tIle-de-France\t3012874\n",
		"/admin2Codes.txt":      "FR.A8.75\tParis\tParis\t2988507\n",
		"/hierarchy.zip":        "PK\x03\x04hierarchy",
	}
	sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := responses[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, ".zip") {
			w.Header().Set("Content-Type", "application/zip")
		} else {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		}
		_, _ = io.WriteString(w, body)
	}))
	defer sourceServer.Close()

	minio := newRecordingObjectStoreServer()
	defer minio.Close()
	clickhouse := newRecordingClickHouseServer()
	defer clickhouse.Close()

	t.Setenv("CLICKHOUSE_HTTP_URL", clickhouse.URL)
	t.Setenv("MINIO_ENDPOINT", minio.URL)
	t.Setenv("MINIO_ACCESS_KEY", "minioadmin")
	t.Setenv("MINIO_SECRET_KEY", "minioadmin")
	t.Setenv("STAGE_BUCKET", "stage")
	t.Setenv("PLACE_STAGE_PREFIX", "place-datasets")
	t.Setenv("GEONAMES_COUNTRY_INFO_URL", sourceServer.URL+"/countryInfo.txt")
	t.Setenv("GEONAMES_ADMIN1_CODES_URL", sourceServer.URL+"/admin1CodesASCII.txt")
	t.Setenv("GEONAMES_ADMIN2_CODES_URL", sourceServer.URL+"/admin2Codes.txt")
	t.Setenv("GEONAMES_HIERARCHY_URL", sourceServer.URL+"/hierarchy.zip")

	if err := runGeoNamesSync(context.Background()); err != nil {
		t.Fatalf("first runGeoNamesSync: %v", err)
	}
	if err := runGeoNamesSync(context.Background()); err != nil {
		t.Fatalf("second runGeoNamesSync: %v", err)
	}
	if len(minio.puts) != 8 {
		t.Fatalf("expected 8 staged uploads across reruns, got %#v", minio.puts)
	}
	firstCycle := []string{
		"/stage/place-datasets/geonames-sync/current/countryInfo.txt",
		"/stage/place-datasets/geonames-sync/current/admin1CodesASCII.txt",
		"/stage/place-datasets/geonames-sync/current/admin2Codes.txt",
		"/stage/place-datasets/geonames-sync/current/hierarchy.zip",
	}
	for idx, want := range append(firstCycle, firstCycle...) {
		if minio.puts[idx].Path != want {
			t.Fatalf("unexpected staged path at %d: got %s want %s", idx, minio.puts[idx].Path, want)
		}
	}
	joined := strings.Join(clickhouse.queries, "\n")
	for _, want := range []string{"geonames-sync", "staged GeoNames admin datasets", "countryInfo.txt", "admin1CodesASCII.txt", "admin2Codes.txt", "hierarchy.zip"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected ClickHouse query to contain %q, got %s", want, joined)
		}
	}
	if count := strings.Count(joined, "staged GeoNames admin datasets"); count != 2 {
		t.Fatalf("expected two successful geonames job-run inserts, got %d in %s", count, joined)
	}
}

type recordedPut struct {
	Path        string
	Body        string
	ContentType string
}

type recordingObjectStoreServer struct {
	*httptest.Server
	mu   sync.Mutex
	puts []recordedPut
}

func newRecordingObjectStoreServer() *recordingObjectStoreServer {
	recorder := &recordingObjectStoreServer{}
	recorder.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		body, _ := io.ReadAll(r.Body)
		recorder.mu.Lock()
		recorder.puts = append(recorder.puts, recordedPut{Path: r.URL.Path, Body: string(body), ContentType: r.Header.Get("Content-Type")})
		recorder.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	return recorder
}

type recordingClickHouseServer struct {
	*httptest.Server
	mu      sync.Mutex
	queries []string
}

func newRecordingClickHouseServer() *recordingClickHouseServer {
	recorder := &recordingClickHouseServer{}
	recorder.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		recorder.mu.Lock()
		recorder.queries = append(recorder.queries, query)
		recorder.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	return recorder
}

func TestRunOnceUsageListsPlaceSyncJobs(t *testing.T) {
	usage := runOnceUsage()
	for _, jobName := range []string{"geoboundaries-sync", "geonames-sync"} {
		if !strings.Contains(usage, jobName) {
			t.Fatalf("expected run-once help to list %s, got %s", jobName, usage)
		}
	}
}
