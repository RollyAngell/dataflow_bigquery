"""Data transformation transforms for the pipeline."""

import logging
from datetime import datetime
from typing import Any, Dict, Iterable

import apache_beam as beam

logger = logging.getLogger(__name__)


class TransformRecord(beam.DoFn):
    """DoFn to transform individual records."""

    def __init__(
        self,
        field_types: Dict[str, str],
        source_file: str,
        normalize_strings: bool = True,
    ):
        self.field_types = field_types
        self.source_file = source_file
        self.normalize_strings = normalize_strings

    def process(self, record: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        try:
            transformed = {}

            for field, value in record.items():
                if value is None:
                    transformed[field] = None
                    continue

                target_type = self.field_types.get(field, "STRING")
                transformed[field] = self._convert_value(value, target_type)

            # Add audit fields
            transformed["_load_timestamp"] = datetime.utcnow().isoformat()
            transformed["_source_file"] = self.source_file

            yield transformed

        except Exception as e:
            logger.error(f"Error transforming record: {str(e)}")
            record["_transform_error"] = True
            record["_error_message"] = str(e)
            yield record

    def _convert_value(self, value: Any, target_type: str) -> Any:
        if value is None:
            return None

        str_value = str(value).strip() if self.normalize_strings else str(value)

        if target_type == "INTEGER":
            return int(float(str_value))
        elif target_type == "FLOAT":
            return float(str_value)
        elif target_type == "BOOLEAN":
            return str_value.lower() in ("true", "1", "yes", "y")
        elif target_type == "TIMESTAMP":
            return self._parse_timestamp(str_value)
        else:
            return str_value

    @staticmethod
    def _parse_timestamp(value: str) -> str:
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
                dt = datetime.strptime(value, fmt)
                return dt.isoformat()
            except ValueError:
                continue
        return value


class TransformRecords(beam.PTransform):
    """Composite PTransform for record transformation."""

    def __init__(
        self,
        field_types: Dict[str, str],
        source_file: str,
        normalize_strings: bool = True,
    ):
        super().__init__()
        self.field_types = field_types
        self.source_file = source_file
        self.normalize_strings = normalize_strings

    def expand(self, pcoll):
        return pcoll | "TransformRecords" >> beam.ParDo(
            TransformRecord(
                field_types=self.field_types,
                source_file=self.source_file,
                normalize_strings=self.normalize_strings,
            )
        )
