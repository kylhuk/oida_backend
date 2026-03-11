package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"global-osint-backend/internal/migrate"
)

const (
	bulkDumpJobName        = "bulk-dump"
	defaultStageObjectName = "bulk_dump.csv"
	defaultStageBucketName = "stage"
	defaultStageEndpoint   = "http://minio:9000"
	defaultBulkDatasetID   = "bulk:example"
)

func init() {
	jobRegistry[bulkDumpJobName] = jobRunner{
		description: "Ingest a staged bulk dump via ClickHouse s3() table function.",
		run:         runBulkDump,
	}
}

func runBulkDump(ctx context.Context) error {
	startedAt := time.Now().UTC().Truncate(time.Millisecond)
	runner := migrate.NewHTTPRunner(controlPlaneClickHouseURL())
	jobID := fmt.Sprintf("job:%s:%d", bulkDumpJobName, startedAt.UnixMilli())
	stageBucket := ctlGetenv("STAGE_BUCKET", defaultStageBucketName)
	stageObject := ctlGetenv("BULK_STAGE_OBJECT", defaultStageObjectName)
	stageURL := stageDatasetURL(stageBucket, stageObject)
	minioAccess := ctlGetenv("MINIO_ACCESS_KEY", "minio")
	minioSecret := ctlGetenv("MINIO_SECRET_KEY", "minio_change_me")
	datasetID := ctlGetenv("BULK_DATASET_ID", defaultBulkDatasetID)
	loadedFrom := fmt.Sprintf("%s/%s", stageBucket, stageObject)
	recordFailure := func(err error, message string, stats map[string]any) error {
		if recordErr := recordJobRun(ctx, runner, jobID, bulkDumpJobName, "failed", startedAt, time.Now().UTC().Truncate(time.Millisecond), message, stats); recordErr != nil {
			return fmt.Errorf("%w (job log failed: %v)", err, recordErr)
		}
		return err
	}
	statements := []string{
		"TRUNCATE TABLE ops.bulk_dump",
		fmt.Sprintf(`INSERT INTO ops.bulk_dump (dataset_id, item_key, item_value, loaded_from, inserted_at)
SELECT dataset_id, item_key, item_value, %s, now64(3)
FROM s3(%s, %s, %s, %s, %s)`,
			sqlString(loadedFrom),
			sqlString(stageURL),
			sqlString(minioAccess),
			sqlString(minioSecret),
			sqlString("CSVWithNames"),
			sqlString("dataset_id String, item_key String, item_value String"),
		),
	}
	for _, statement := range statements {
		if err := runner.ApplySQL(ctx, statement); err != nil {
			return recordFailure(err, "apply staged bulk dump", map[string]any{"stage_url": stageURL, "stage_bucket": stageBucket, "stage_object": stageObject})
		}
	}
	stats := map[string]any{
		"stage_url":             stageURL,
		"stage_bucket":          stageBucket,
		"stage_object":          stageObject,
		"expected_dataset_id":   datasetID,
		"staged_loaded_from":    loadedFrom,
	}
	if err := recordJobRun(ctx, runner, jobID, bulkDumpJobName, "success", startedAt, time.Now().UTC().Truncate(time.Millisecond), "materialized bulk staged dump", stats); err != nil {
		return err
	}
	return nil
}

func stageDatasetURL(bucket, objectPath string) string {
	endpoint := strings.TrimRight(ctlGetenv("MINIO_ENDPOINT", defaultStageEndpoint), "/")
	if objectPath == "" {
		objectPath = defaultStageObjectName
	}
	trimmed := strings.TrimLeft(objectPath, "/")
	return fmt.Sprintf("%s/%s/%s", endpoint, bucket, trimmed)
}

func ctlGetenv(key, def string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return def
}
