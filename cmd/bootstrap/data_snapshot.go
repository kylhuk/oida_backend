package main

import (
	"context"
	"fmt"
	"log"
)

func seedLiveSnapshot(ctx context.Context, store sourceRegistryStore) error {
	sql := `INSERT INTO meta.data_snapshot
(snapshot_id, captured_at, tables, description, schema_version, record_version, api_contract_version)
VALUES ('live', now64(3), [], 'live data - no temporal filter applied', 1, 1, 1)`
	if err := store.ApplySQL(ctx, sql); err != nil {
		return fmt.Errorf("seed live snapshot: %w", err)
	}
	log.Println("bootstrap: seeded meta.data_snapshot live row")
	return nil
}
