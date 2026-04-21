ALTER TABLE meta.source_registry
UPDATE
    lifecycle_state = 'approved_disabled',
    crawl_enabled = 0,
    review_status = 'review_required',
    review_notes = multiIf(
        source_id = 'catalog:auto:maritime-ocean-and-coastal-sources-equasis',
            'deferred in urgent phase-1: login-gated contract not implemented',
        source_id = 'catalog:auto:maritime-ocean-and-coastal-sources-imo-gisis',
            'deferred in urgent phase-1: interactive contract not implemented',
        review_notes
    )
WHERE source_id IN (
    'catalog:auto:maritime-ocean-and-coastal-sources-equasis',
    'catalog:auto:maritime-ocean-and-coastal-sources-imo-gisis'
)
SETTINGS mutations_sync = 2;
