"""Logging and metrics utilities for the pipeline."""

import logging
from datetime import datetime
from typing import Any, Dict, Optional

from apache_beam.metrics import Metrics

logger = logging.getLogger(__name__)


class PipelineMetrics:
    """Centralized metrics tracking for the pipeline."""

    NAMESPACE = "dataflow_batch_pipeline"

    records_read = Metrics.counter(NAMESPACE, "records_read")
    records_valid = Metrics.counter(NAMESPACE, "records_valid")
    records_invalid = Metrics.counter(NAMESPACE, "records_invalid")
    records_written_bq = Metrics.counter(NAMESPACE, "records_written_bigquery")
    records_written_dl = Metrics.counter(NAMESPACE, "records_written_dead_letter")

    validation_missing_field = Metrics.counter(NAMESPACE, "validation_missing_field")
    validation_type_error = Metrics.counter(NAMESPACE, "validation_type_error")
    validation_format_error = Metrics.counter(NAMESPACE, "validation_format_error")

    @classmethod
    def increment_read(cls, count: int = 1) -> None:
        cls.records_read.inc(count)

    @classmethod
    def increment_valid(cls, count: int = 1) -> None:
        cls.records_valid.inc(count)

    @classmethod
    def increment_invalid(cls, count: int = 1) -> None:
        cls.records_invalid.inc(count)

    @classmethod
    def increment_written_bq(cls, count: int = 1) -> None:
        cls.records_written_bq.inc(count)

    @classmethod
    def increment_written_dl(cls, count: int = 1) -> None:
        cls.records_written_dl.inc(count)


def create_error_record(
    original_record: Dict[str, Any],
    error_type: str,
    error_message: str,
    field_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Create a standardized error record for dead letter output."""
    return {
        "original_record": original_record,
        "error_type": error_type,
        "error_message": error_message,
        "field_name": field_name,
        "timestamp": datetime.utcnow().isoformat(),
    }


def setup_logging(level: int = logging.INFO) -> None:
    """Configure logging for the pipeline."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logging.getLogger("apache_beam").setLevel(logging.WARNING)
    logging.getLogger("google").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
