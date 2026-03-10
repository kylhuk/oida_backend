package migrate

import (
	"strings"
	"testing"
)

func TestClusterComposeDefinesOptionalScaleOutTopology(t *testing.T) {
	compose := readRepoFile(t, "docker-compose.cluster.yml")

	for _, fragment := range []string{
		"name: osint-backend-cluster",
		"clickhouse-keeper-1:",
		"clickhouse-keeper-2:",
		"clickhouse-keeper-3:",
		"clickhouse-01:",
		"clickhouse-02:",
		"clickhouse-03:",
		"clickhouse-04:",
		"clickhouse/clickhouse-keeper:24.8.14.39",
		"clickhouse/clickhouse-server:24.8.14.39",
		"./infra/clickhouse/cluster/backups:/var/lib/clickhouse/user_files",
	} {
		if !strings.Contains(compose, fragment) {
			t.Fatalf("cluster compose missing fragment %q", fragment)
		}
	}

	if strings.Contains(compose, ":latest") {
		t.Fatal("cluster compose must pin images")
	}
}

func TestClusterConfigsDefineKeeperQuorumAndShardReplicaMacros(t *testing.T) {
	common := readRepoFile(t, "infra", "clickhouse", "cluster", "config", "00-cluster-common.xml")

	for _, fragment := range []string{
		"<osint_cluster_2s2r>",
		"<internal_replication>true</internal_replication>",
		"<host>clickhouse-01</host>",
		"<host>clickhouse-02</host>",
		"<host>clickhouse-03</host>",
		"<host>clickhouse-04</host>",
		"<host>clickhouse-keeper-1</host>",
		"<host>clickhouse-keeper-2</host>",
		"<host>clickhouse-keeper-3</host>",
	} {
		if !strings.Contains(common, fragment) {
			t.Fatalf("cluster config missing fragment %q", fragment)
		}
	}

	for _, path := range []string{
		"infra/clickhouse/cluster/config/10-clickhouse-01.xml",
		"infra/clickhouse/cluster/config/10-clickhouse-02.xml",
		"infra/clickhouse/cluster/config/10-clickhouse-03.xml",
		"infra/clickhouse/cluster/config/10-clickhouse-04.xml",
	} {
		config := readRepoFile(t, strings.Split(path, "/")...)
		for _, fragment := range []string{"<cluster>osint_cluster_2s2r</cluster>", "<shard>", "<replica>"} {
			if !strings.Contains(config, fragment) {
				t.Fatalf("%s missing fragment %q", path, fragment)
			}
		}
	}
}

func TestClusterSQLDefinesReplicatedLocalAndDistributedTables(t *testing.T) {
	sqlFiles := []string{
		"infra/clickhouse/cluster/sql/0005-meta-source-registry-local.sql",
		"infra/clickhouse/cluster/sql/0006-meta-source-registry-distributed.sql",
		"infra/clickhouse/cluster/sql/0007-bronze-raw-document-local.sql",
		"infra/clickhouse/cluster/sql/0008-bronze-raw-document-distributed.sql",
		"infra/clickhouse/cluster/sql/0009-silver-fact-event-local.sql",
		"infra/clickhouse/cluster/sql/0010-silver-fact-event-distributed.sql",
		"infra/clickhouse/cluster/sql/0011-gold-metric-state-local.sql",
		"infra/clickhouse/cluster/sql/0012-gold-metric-state-distributed.sql",
		"infra/clickhouse/cluster/sql/0013-gold-metric-snapshot-local.sql",
		"infra/clickhouse/cluster/sql/0014-gold-metric-snapshot-distributed.sql",
	}

	combined := ""
	for _, path := range sqlFiles {
		combined += readRepoFile(t, strings.Split(path, "/")...)
		combined += "\n"
	}

	for _, fragment := range []string{
		"ReplicatedReplacingMergeTree",
		"ReplicatedMergeTree",
		"ReplicatedAggregatingMergeTree",
		"ENGINE = Distributed('osint_cluster_2s2r'",
		"bronze.raw_document_local",
		"silver.fact_event_local",
		"gold.metric_state_local",
		"gold.metric_snapshot_local",
		"TTL toDateTime(fetched_at) + INTERVAL 180 DAY DELETE",
		"TTL toDateTime(starts_at) + INTERVAL 1095 DAY DELETE",
		"TTL toDateTime(window_start) + INTERVAL 730 DAY DELETE",
		"TTL toDateTime(snapshot_at) + INTERVAL 365 DAY DELETE",
	} {
		if !strings.Contains(combined, fragment) {
			t.Fatalf("cluster SQL missing fragment %q", fragment)
		}
	}
}

func TestScaleOutDocsFreezeSourceOfTruthAndDrPolicy(t *testing.T) {
	readme := readRepoFile(t, "infra", "clickhouse", "cluster", "README.md")
	runbook := readRepoFile(t, "docs", "runbooks", "cluster-scale-out.md")

	for _, doc := range []string{readme, runbook} {
		for _, fragment := range []string{
			"single-node",
			"Distributed",
			"source of truth",
			"backup",
			"restore",
			"skip_unavailable_shards=0",
			"insert_distributed_sync=1",
		} {
			if !strings.Contains(doc, fragment) {
				t.Fatalf("documentation missing fragment %q", fragment)
			}
		}
	}
}

func TestCostControlMigrationAddsRawRetentionTTL(t *testing.T) {
	migration := readRepoFile(t, "migrations", "clickhouse", "0008_cost_controls.sql")

	for _, fragment := range []string{
		"ALTER TABLE ops.fetch_log",
		"MODIFY TTL toDateTime(fetched_at) + INTERVAL 180 DAY DELETE",
		"ALTER TABLE bronze.raw_document",
	} {
		if !strings.Contains(migration, fragment) {
			t.Fatalf("cost control migration missing fragment %q", fragment)
		}
	}
}
