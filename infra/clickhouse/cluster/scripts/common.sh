#!/usr/bin/env bash

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
compose_file="$repo_root/docker-compose.cluster.yml"
cluster_name="osint_cluster_2s2r"
cluster_http="http://localhost:19123/"
backup_dir="$repo_root/infra/clickhouse/cluster/backups/task-26"

run_query() {
    curl -fsS --data-binary "$1" "$cluster_http"
}

run_statement() {
    curl -fsS --data-binary "$1" "$cluster_http" >/dev/null
}

trim_newline() {
    local value="$1"
    printf '%s' "${value%$'\n'}"
}

wait_for_http_port() {
    local port="$1"

    for _ in $(seq 1 120); do
        if curl -fsS "http://localhost:${port}/ping" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done

    printf 'timed out waiting for port %s\n' "$port" >&2
    return 1
}

start_cluster() {
    docker compose -f "$compose_file" up -d \
        clickhouse-keeper-1 \
        clickhouse-keeper-2 \
        clickhouse-keeper-3 \
        clickhouse-01 \
        clickhouse-02 \
        clickhouse-03 \
        clickhouse-04 >/dev/null

    wait_for_http_port 19123
    wait_for_http_port 29123
    wait_for_http_port 39123
    wait_for_http_port 49123
}

wait_for_replication() {
    local pending

    for _ in $(seq 1 120); do
        pending="$(run_query "SELECT count() FROM clusterAllReplicas('$cluster_name', 'system', 'replicas') WHERE database IN ('meta', 'bronze', 'silver', 'gold') AND table IN ('source_registry_local', 'raw_document_local', 'fact_event_local', 'metric_state_local', 'metric_snapshot_local') AND (is_session_expired OR is_readonly OR active_replicas < total_replicas OR queue_size > 0) FORMAT TabSeparated")"
        pending="$(trim_newline "$pending")"
        if [ "$pending" = "0" ]; then
            return 0
        fi
        sleep 1
    done

    printf 'replication did not settle\n' >&2
    return 1
}

pick_key() {
    local prefix="$1"
    local target_shard="$2"
    local candidate=""
    local shard=""

    for i in $(seq 0 200); do
        candidate="${prefix}-${i}"
        shard="$(run_query "SELECT cityHash64('$candidate') % 2 FORMAT TabSeparated")"
        shard="$(trim_newline "$shard")"
        if [ "$shard" = "$target_shard" ]; then
            printf '%s' "$candidate"
            return 0
        fi
    done

    printf 'unable to find key for shard %s\n' "$target_shard" >&2
    return 1
}

truncate_fixture_tables() {
    run_statement "TRUNCATE TABLE bronze.raw_document_local ON CLUSTER '$cluster_name' SYNC"
    run_statement "TRUNCATE TABLE silver.fact_event_local ON CLUSTER '$cluster_name' SYNC"
    run_statement "TRUNCATE TABLE gold.metric_snapshot_local ON CLUSTER '$cluster_name' SYNC"
}

seed_fixture() {
    local raw0 raw1 event0 event1 snapshot0 snapshot1

    raw0="$(pick_key raw 0)"
    raw1="$(pick_key raw 1)"
    event0="$(pick_key event 0)"
    event1="$(pick_key event 1)"
    snapshot0="$(pick_key snapshot 0)"
    snapshot1="$(pick_key snapshot 1)"

    truncate_fixture_tables

    run_statement "INSERT INTO bronze.raw_document_all (raw_id, source_id, url, fetched_at, status_code, content_type, content_hash, body_bytes, object_key, fetch_metadata) FORMAT Values ('$raw0', 'fixture:cluster', 'https://fixture.example/raw0', '2026-03-10 10:00:00.000', 200, 'application/json', 'hash-$raw0', 512, 'raw/$raw0.json', '{}'), ('$raw1', 'fixture:cluster', 'https://fixture.example/raw1', '2026-03-10 10:05:00.000', 200, 'application/json', 'hash-$raw1', 768, 'raw/$raw1.json', '{}')"

    run_statement "INSERT INTO silver.fact_event_all (event_id, source_id, event_type, event_subtype, place_id, parent_place_chain, starts_at, ends_at, status, confidence_band, impact_score, schema_version, attrs, evidence) FORMAT Values ('$event0', 'fixture:cluster', 'fixture_event', 'alpha', 'plc:alpha', ['plc:world'], '2026-03-10 10:15:00.000', NULL, 'active', 'high', 0.75, 1, '{}', '[]'), ('$event1', 'fixture:cluster', 'fixture_event', 'beta', 'plc:beta', ['plc:world'], '2026-03-10 10:20:00.000', NULL, 'active', 'medium', 0.55, 1, '{}', '[]')"

    run_statement "INSERT INTO gold.metric_snapshot_all (snapshot_id, metric_id, subject_grain, subject_id, place_id, window_grain, window_start, window_end, snapshot_at, metric_value, metric_delta, rank, schema_version, attrs, evidence) FORMAT Values ('$snapshot0', 'event_count', 'place', 'plc:alpha', 'plc:alpha', 'day', '2026-03-10 00:00:00.000', '2026-03-11 00:00:00.000', '2026-03-10 10:30:00.000', 11.0, 2.0, 1, 1, '{}', '[]'), ('$snapshot1', 'event_count', 'place', 'plc:beta', 'plc:beta', 'day', '2026-03-10 00:00:00.000', '2026-03-11 00:00:00.000', '2026-03-10 10:31:00.000', 7.0, 1.0, 2, 1, '{}', '[]')"

    wait_for_replication
}
