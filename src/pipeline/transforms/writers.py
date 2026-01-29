"""Writer transforms for the pipeline."""

import json
import logging
from datetime import datetime
from typing import Any, Dict, Iterable, Optional

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.io.gcp.bigquery import WriteToBigQuery as BeamWriteToBigQuery

from pipeline.utils.logging_utils import PipelineMetrics
from pipeline.utils.schema import get_table_schema

logger = logging.getLogger(__name__)


class FormatForBigQuery(beam.DoFn):
    """DoFn to format records for BigQuery insertion."""

    def __init__(self, schema_fields: list):
        self.schema_fields = set(schema_fields)

    def process(self, record: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        formatted = {k: v for k, v in record.items() if k in self.schema_fields}

        if "_load_timestamp" in formatted and formatted["_load_timestamp"]:
            ts = formatted["_load_timestamp"]
            if isinstance(ts, str) and "T" not in ts:
                formatted["_load_timestamp"] = ts.replace(" ", "T")

        PipelineMetrics.increment_written_bq()
        yield formatted


class WriteToBigQuery(beam.PTransform):
    """Composite PTransform to write records to BigQuery."""

    def __init__(
        self,
        table: str,
        schema_path: Optional[str] = None,
        write_disposition: str = "WRITE_APPEND",
        create_disposition: str = "CREATE_IF_NEEDED",
        method: str = "DEFAULT",
    ):
        super().__init__()
        self.table = table
        self.schema_path = schema_path
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition
        self.method = method

    def expand(self, pcoll):
        schema = get_table_schema(self.schema_path)
        schema_fields = [f["name"] for f in schema["fields"]]

        formatted = pcoll | "FormatForBigQuery" >> beam.ParDo(
            FormatForBigQuery(schema_fields)
        )

        return formatted | "WriteToBigQuery" >> BeamWriteToBigQuery(
            table=self.table,
            schema=schema,
            write_disposition=getattr(BigQueryDisposition, self.write_disposition),
            create_disposition=getattr(BigQueryDisposition, self.create_disposition),
            method=self.method,
            additional_bq_parameters={
                "timePartitioning": {
                    "type": "DAY",
                    "field": "_load_timestamp",
                }
            },
        )


class FormatDeadLetter(beam.DoFn):
    """DoFn to format error records for dead letter output."""

    def process(self, error_record: Dict[str, Any]) -> Iterable[str]:
        PipelineMetrics.increment_written_dl()
        yield json.dumps(error_record, default=str, ensure_ascii=False)


class WriteToDeadLetter(beam.PTransform):
    """Composite PTransform to write invalid records to GCS."""

    def __init__(
        self,
        output_path: str,
        file_name_suffix: str = ".json",
        shard_name_template: str = "-SSSSS-of-NNNNN",
    ):
        super().__init__()
        self.output_path = output_path
        self.file_name_suffix = file_name_suffix
        self.shard_name_template = shard_name_template

    def expand(self, pcoll):
        timestamp = datetime.utcnow().strftime("%Y/%m/%d/%H%M%S")
        output_path = f"{self.output_path}/{timestamp}/errors"

        return (
            pcoll
            | "FormatDeadLetter" >> beam.ParDo(FormatDeadLetter())
            | "WriteToGCS"
            >> WriteToText(
                file_path_prefix=output_path,
                file_name_suffix=self.file_name_suffix,
                shard_name_template=self.shard_name_template,
            )
        )
