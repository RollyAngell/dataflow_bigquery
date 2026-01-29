"""CSV Reader transforms for the pipeline."""

import csv
import logging
from io import StringIO
from typing import Any, Dict, Iterable, List, Optional

import apache_beam as beam
from apache_beam.io import ReadFromText

from pipeline.utils.logging_utils import PipelineMetrics

logger = logging.getLogger(__name__)


class ParseCSVLine(beam.DoFn):
    """DoFn to parse individual CSV lines into dictionaries."""

    def __init__(
        self,
        headers: List[str],
        delimiter: str = ",",
        null_values: Optional[List[str]] = None,
    ):
        self.headers = headers
        self.delimiter = delimiter
        self.null_values = null_values or [
            "",
            "NULL",
            "null",
            "None",
            "NA",
            "N/A",
            "nan",
        ]

    def process(self, line: str) -> Iterable[Dict[str, Any]]:
        try:
            reader = csv.reader(StringIO(line), delimiter=self.delimiter)
            values = next(reader)

            record = {}
            for i, header in enumerate(self.headers):
                if i < len(values):
                    value = values[i].strip()
                    record[header] = None if value in self.null_values else value
                else:
                    record[header] = None

            PipelineMetrics.increment_read()
            yield record

        except Exception as e:
            logger.error(f"Error parsing CSV line: {line[:100]}... Error: {str(e)}")
            yield {
                "_parse_error": True,
                "_error_message": str(e),
                "_raw_line": line[:1000],
            }


class ReadCSVFromGCS(beam.PTransform):
    """Composite PTransform to read CSV files from GCS."""

    def __init__(
        self,
        file_path: str,
        headers: List[str],
        delimiter: str = ",",
        skip_header: bool = True,
        null_values: Optional[List[str]] = None,
    ):
        super().__init__()
        self.file_path = file_path
        self.headers = headers
        self.delimiter = delimiter
        self.skip_header = skip_header
        self.null_values = null_values

    def expand(self, pcoll):
        lines = pcoll | "ReadFromGCS" >> ReadFromText(self.file_path)

        if self.skip_header:
            lines = lines | "SkipHeader" >> beam.Filter(
                lambda line, headers=self.headers: not self._is_header_line(
                    line, headers, self.delimiter
                )
            )

        records = lines | "ParseCSVLines" >> beam.ParDo(
            ParseCSVLine(
                headers=self.headers,
                delimiter=self.delimiter,
                null_values=self.null_values,
            )
        )

        return records

    @staticmethod
    def _is_header_line(line: str, headers: List[str], delimiter: str) -> bool:
        try:
            reader = csv.reader(StringIO(line), delimiter=delimiter)
            values = [v.strip().lower() for v in next(reader)]
            expected = [h.strip().lower() for h in headers]
            return values == expected
        except Exception:
            return False
