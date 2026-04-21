from __future__ import annotations

import logging

from data_platform.request_context import CorrelationIdFilter


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(request_id)s | %(message)s",
        force=True,
    )
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        if not any(isinstance(existing_filter, CorrelationIdFilter) for existing_filter in handler.filters):
            handler.addFilter(CorrelationIdFilter())
