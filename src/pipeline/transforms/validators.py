"""Data validation transforms for the pipeline."""

import logging
import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

import apache_beam as beam
from apache_beam import pvalue

from pipeline.utils.logging_utils import PipelineMetrics, create_error_record

logger = logging.getLogger(__name__)

VALID_TAG = "valid"
INVALID_TAG = "invalid"


class ValidateRecord(beam.DoFn):
    """DoFn to validate individual records."""

    def __init__(
        self,
        required_fields: List[str],
        field_types: Optional[Dict[str, str]] = None,
        max_field_lengths: Optional[Dict[str, int]] = None,
    ):
        self.required_fields = required_fields
        self.field_types = field_types or {}
        self.max_field_lengths = max_field_lengths or {}

    def process(self, record: Dict[str, Any]) -> Iterable[pvalue.TaggedOutput]:
        if record.get("_parse_error"):
            PipelineMetrics.increment_invalid()
            yield pvalue.TaggedOutput(
                INVALID_TAG,
                create_error_record(
                    record,
                    "PARSE_ERROR",
                    record.get("_error_message", "Unknown parse error"),
                ),
            )
            return

        errors = []

        # Validate required fields
        for field in self.required_fields:
            if field not in record or record[field] is None:
                errors.append(f"Missing required field: {field}")
                PipelineMetrics.validation_missing_field.inc()

        # Validate data types
        for field, expected_type in self.field_types.items():
            if field in record and record[field] is not None:
                type_error = self._validate_type(field, record[field], expected_type)
                if type_error:
                    errors.append(type_error)
                    PipelineMetrics.validation_type_error.inc()

        # Validate field lengths
        for field, max_length in self.max_field_lengths.items():
            if field in record and record[field] is not None:
                if len(str(record[field])) > max_length:
                    errors.append(f"Field '{field}' exceeds max length {max_length}")

        # Validate email format if present
        if "email" in record and record["email"] is not None:
            if not self._is_valid_email(record["email"]):
                errors.append(f"Invalid email format: {record['email']}")
                PipelineMetrics.validation_format_error.inc()

        if errors:
            PipelineMetrics.increment_invalid()
            yield pvalue.TaggedOutput(
                INVALID_TAG,
                create_error_record(record, "VALIDATION_ERROR", "; ".join(errors)),
            )
        else:
            PipelineMetrics.increment_valid()
            yield pvalue.TaggedOutput(VALID_TAG, record)

    def _validate_type(
        self, field: str, value: Any, expected_type: str
    ) -> Optional[str]:
        try:
            if expected_type == "INTEGER":
                int(value)
            elif expected_type == "FLOAT":
                float(value)
            elif expected_type == "BOOLEAN":
                if str(value).lower() not in ("true", "false", "1", "0", "yes", "no"):
                    return f"Field '{field}' has invalid boolean value: {value}"
            elif expected_type == "TIMESTAMP":
                self._parse_timestamp(value)
            return None
        except (ValueError, TypeError):
            return f"Field '{field}' cannot be converted to {expected_type}: {value}"

    @staticmethod
    def _parse_timestamp(value: str) -> datetime:
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%m/%d/%Y",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(str(value), fmt)
            except ValueError:
                continue
        raise ValueError(f"Cannot parse timestamp: {value}")

    @staticmethod
    def _is_valid_email(email: str) -> bool:
        pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        return bool(re.match(pattern, str(email)))


class ValidateRecords(beam.PTransform):
    """Composite PTransform for record validation with branching."""

    def __init__(
        self,
        required_fields: List[str],
        field_types: Optional[Dict[str, str]] = None,
        max_field_lengths: Optional[Dict[str, int]] = None,
    ):
        super().__init__()
        self.required_fields = required_fields
        self.field_types = field_types
        self.max_field_lengths = max_field_lengths

    def expand(self, pcoll):
        return pcoll | "ValidateRecords" >> beam.ParDo(
            ValidateRecord(
                required_fields=self.required_fields,
                field_types=self.field_types,
                max_field_lengths=self.max_field_lengths,
            )
        ).with_outputs(VALID_TAG, INVALID_TAG)
