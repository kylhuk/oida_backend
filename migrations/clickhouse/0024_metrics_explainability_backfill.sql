ALTER TABLE meta.metric_registry
UPDATE attrs = if(
    JSONHas(attrs, 'explainability'),
    attrs,
    if(
        trim(attrs) = '{}',
        concat(
            '{"explainability":{',
            '"includes_confidence":true,',
            '"includes_feature_contributions":true,',
            '"includes_evidence_refs":true,',
            '"summary":"',
            replaceAll(if(JSONExtractString(attrs, 'description') = '', metric_id, JSONExtractString(attrs, 'description')), '"', '\\"'),
            '"}}'
        ),
        replaceRegexpOne(
            attrs,
            '\\}$',
            concat(
                ',"explainability":{',
                '"includes_confidence":true,',
                '"includes_feature_contributions":true,',
                '"includes_evidence_refs":true,',
                '"summary":"',
                replaceAll(if(JSONExtractString(attrs, 'description') = '', metric_id, JSONExtractString(attrs, 'description')), '"', '\\"'),
                '"}}'
            )
        )
    )
)
WHERE NOT JSONHas(attrs, 'explainability');

ALTER TABLE gold.metric_snapshot
UPDATE attrs = if(
    JSONHas(attrs, 'explainability'),
    attrs,
    if(
        trim(attrs) = '{}',
        concat(
            '{"explainability":{',
            '"includes_confidence":true,',
            '"includes_feature_contributions":true,',
            '"includes_evidence_refs":true,',
            '"summary":"',
            replaceAll(metric_id, '"', '\\"'),
            '"}}'
        ),
        replaceRegexpOne(
            attrs,
            '\\}$',
            concat(
                ',"explainability":{',
                '"includes_confidence":true,',
                '"includes_feature_contributions":true,',
                '"includes_evidence_refs":true,',
                '"summary":"',
                replaceAll(metric_id, '"', '\\"'),
                '"}}'
            )
        )
    )
),
evidence = if(
    trim(evidence) = '[]',
    concat('[{"kind":"metric_registry","ref":"', replaceAll(metric_id, '"', '\\"'), '","value":"snapshot_support"}]'),
    evidence
)
WHERE NOT JSONHas(attrs, 'explainability') OR trim(evidence) = '[]';
