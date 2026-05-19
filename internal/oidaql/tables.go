package oidaql

// LogicalToPhysical maps the logical table names used in OIDA-QL queries
// to their physical ClickHouse views. Logical names are those emitted by
// the frontend's oida-ql-emitter.ts.
var LogicalToPhysical = map[string]string{
	"entities":       "gold.api_v1_entities",
	"places":         "gold.api_v1_places",
	"events":         "gold.api_v1_events",
	"observations":   "gold.api_v1_observations",
	"track_points":   "silver.fact_track_point",
	"track_segments": "gold.api_v1_tracks",
	"artifacts":      "gold.api_v1_artifacts",
	"metrics":        "gold.api_v1_metrics",
}
