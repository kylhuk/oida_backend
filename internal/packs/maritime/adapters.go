package maritime

import (
	"sort"
	"time"

	"global-osint-backend/internal/canonical"
)

const (
	domainFamily       = "maritime"
	schemaVersion      = uint32(1)
	apiContractVersion = uint32(1)
)

type Adapter struct {
	AdapterID           string               `json:"adapter_id"`
	DisplayName         string               `json:"display_name"`
	Domain              string               `json:"domain"`
	DomainFamily        string               `json:"domain_family"`
	SourceClass         string               `json:"source_class"`
	Entrypoints         []string             `json:"entrypoints"`
	AuthMode            string               `json:"auth_mode"`
	AuthConfig          map[string]any       `json:"auth_config_json,omitempty"`
	FormatHint          string               `json:"format_hint"`
	RobotsPolicy        string               `json:"robots_policy"`
	RefreshStrategy     string               `json:"refresh_strategy"`
	RequestsPerMinute   uint32               `json:"requests_per_minute"`
	BurstSize           uint16               `json:"burst_size"`
	RetentionClass      string               `json:"retention_class"`
	License             string               `json:"license"`
	TermsURL            string               `json:"terms_url"`
	AttributionRequired bool                 `json:"attribution_required"`
	GeoScope            string               `json:"geo_scope"`
	Priority            uint16               `json:"priority"`
	ParserID            string               `json:"parser_id"`
	EntityTypes         []string             `json:"entity_types"`
	ExpectedPlaceTypes  []string             `json:"expected_place_types"`
	SupportsHistorical  bool                 `json:"supports_historical"`
	SupportsDelta       bool                 `json:"supports_delta"`
	BackfillPriority    uint16               `json:"backfill_priority"`
	ConfidenceBaseline  float64              `json:"confidence_baseline"`
	Enabled             bool                 `json:"enabled"`
	Attrs               map[string]any       `json:"attrs,omitempty"`
	Evidence            []canonical.Evidence `json:"evidence,omitempty"`
	SchemaVersion       uint32               `json:"schema_version"`
	RecordVersion       uint64               `json:"record_version"`
	APIContractVersion  uint32               `json:"api_contract_version"`
	UpdatedAt           time.Time            `json:"updated_at"`
}

func DefaultAdapters(now time.Time) []Adapter {
	now = now.UTC().Truncate(time.Millisecond)
	adapters := []Adapter{
		{
			AdapterID:           "maritime:ais:community",
			DisplayName:         "Community AIS telemetry",
			Domain:              "data.aishub.net",
			SourceClass:         "streaming_public_telemetry",
			Entrypoints:         []string{"https://data.aishub.net/ws.php"},
			AuthMode:            "user_supplied_key",
			AuthConfig:          map[string]any{"credential_kind": "api_key", "delivery": "query_param", "parameter_name": "key", "rotation": "per_user"},
			FormatHint:          "json",
			RobotsPolicy:        "respect",
			RefreshStrategy:     "streaming_poll",
			RequestsPerMinute:   120,
			BurstSize:           20,
			RetentionClass:      "hot",
			License:             "community",
			TermsURL:            "https://www.aishub.net/terms",
			AttributionRequired: true,
			GeoScope:            "global",
			Priority:            15,
			ParserID:            "parser:json",
			EntityTypes:         []string{"vessel", "track", "event"},
			ExpectedPlaceTypes:  []string{"port", "anchorage", "admin0"},
			SupportsHistorical:  false,
			SupportsDelta:       true,
			BackfillPriority:    20,
			ConfidenceBaseline:  0.78,
			Enabled:             true,
			Attrs: map[string]any{
				"capabilities": []string{"position_updates", "voyage_context", "ais_gap_detection"},
				"pack":         domainFamily,
			},
		},
		{
			AdapterID:           "maritime:port:unlocode",
			DisplayName:         "UN/LOCODE port metadata",
			Domain:              "service.unece.org",
			SourceClass:         "bulk_file",
			Entrypoints:         []string{"https://service.unece.org/trade/locode/"},
			AuthMode:            "none",
			FormatHint:          "csv",
			RobotsPolicy:        "respect",
			RefreshStrategy:     "scheduled_snapshot",
			RequestsPerMinute:   10,
			BurstSize:           2,
			RetentionClass:      "warm",
			License:             "public",
			TermsURL:            "https://unece.org/trade/uncefact/unlocode",
			AttributionRequired: true,
			GeoScope:            "global",
			Priority:            25,
			ParserID:            "parser:csv",
			EntityTypes:         []string{"port", "place"},
			ExpectedPlaceTypes:  []string{"port", "admin1", "admin0"},
			SupportsHistorical:  true,
			SupportsDelta:       false,
			BackfillPriority:    70,
			ConfidenceBaseline:  0.92,
			Enabled:             true,
			Attrs: map[string]any{
				"capabilities": []string{"unlocode_lookup", "port_name_normalization", "country_port_linking"},
				"pack":         domainFamily,
			},
		},
		{
			AdapterID:           "maritime:registry:vessel",
			DisplayName:         "Vessel registry metadata",
			Domain:              "gisis.imo.org",
			SourceClass:         "structured_api",
			Entrypoints:         []string{"https://gisis.imo.org/Public/SHIPS/"},
			AuthMode:            "none",
			FormatHint:          "json",
			RobotsPolicy:        "respect",
			RefreshStrategy:     "scheduled_snapshot",
			RequestsPerMinute:   12,
			BurstSize:           3,
			RetentionClass:      "warm",
			License:             "public_metadata",
			TermsURL:            "https://www.imo.org/",
			AttributionRequired: true,
			GeoScope:            "global",
			Priority:            20,
			ParserID:            "parser:json",
			EntityTypes:         []string{"vessel", "organization"},
			ExpectedPlaceTypes:  []string{"port", "admin0"},
			SupportsHistorical:  true,
			SupportsDelta:       true,
			BackfillPriority:    80,
			ConfidenceBaseline:  0.88,
			Enabled:             true,
			Attrs: map[string]any{
				"capabilities": []string{"imo_lookup", "mmsi_enrichment", "flag_history_seed"},
				"pack":         domainFamily,
			},
		},
		{
			AdapterID:           "maritime:sanctions:entity_graph",
			DisplayName:         "Sanctions and ownership enrichment",
			Domain:              "opensanctions.org",
			SourceClass:         "structured_api",
			Entrypoints:         []string{"https://www.opensanctions.org/datasets/default/"},
			AuthMode:            "none",
			FormatHint:          "json",
			RobotsPolicy:        "respect",
			RefreshStrategy:     "scheduled_snapshot",
			RequestsPerMinute:   30,
			BurstSize:           5,
			RetentionClass:      "warm",
			License:             "open-data",
			TermsURL:            "https://www.opensanctions.org/legal/",
			AttributionRequired: true,
			GeoScope:            "global",
			Priority:            30,
			ParserID:            "parser:json",
			EntityTypes:         []string{"vessel", "organization", "person"},
			ExpectedPlaceTypes:  []string{"admin0"},
			SupportsHistorical:  true,
			SupportsDelta:       true,
			BackfillPriority:    60,
			ConfidenceBaseline:  0.85,
			Enabled:             true,
			Attrs: map[string]any{
				"capabilities": []string{"sanctions_match", "beneficial_owner_linking", "watchlist_enrichment"},
				"pack":         domainFamily,
			},
		},
	}

	sort.Slice(adapters, func(i, j int) bool {
		return adapters[i].AdapterID < adapters[j].AdapterID
	})
	for i := range adapters {
		adapters[i].DomainFamily = domainFamily
		adapters[i].SchemaVersion = schemaVersion
		adapters[i].RecordVersion = uint64(i + 1)
		adapters[i].APIContractVersion = apiContractVersion
		adapters[i].UpdatedAt = now
		adapters[i].Entrypoints = copyStringSlice(adapters[i].Entrypoints)
		adapters[i].EntityTypes = copyStringSlice(adapters[i].EntityTypes)
		adapters[i].ExpectedPlaceTypes = copyStringSlice(adapters[i].ExpectedPlaceTypes)
		adapters[i].AuthConfig = cloneAnyMap(adapters[i].AuthConfig)
		adapters[i].Attrs = cloneAnyMap(adapters[i].Attrs)
		adapters[i].Evidence = []canonical.Evidence{{
			Kind:  "adapter_spec",
			Ref:   adapters[i].AdapterID,
			Value: adapters[i].SourceClass,
			Attrs: map[string]any{
				"auth_mode":     adapters[i].AuthMode,
				"domain_family": adapters[i].DomainFamily,
			},
		}}
	}
	return adapters
}

func AdapterByID(adapters []Adapter, adapterID string) (Adapter, bool) {
	for _, adapter := range adapters {
		if adapter.AdapterID == adapterID {
			return adapter, true
		}
	}
	return Adapter{}, false
}
