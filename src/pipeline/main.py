"""Main entry point for the Dataflow batch pipeline."""

import argparse
import logging
from typing import List, Optional

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from pipeline.options import DataflowBatchOptions
from pipeline.transforms.readers import ReadCSVFromGCS
from pipeline.transforms.transformers import TransformRecords
from pipeline.transforms.validators import INVALID_TAG, VALID_TAG, ValidateRecords
from pipeline.transforms.writers import WriteToBigQuery, WriteToDeadLetter
from pipeline.utils.logging_utils import setup_logging
from pipeline.utils.schema import get_field_types, get_required_fields

logger = logging.getLogger(__name__)

DEFAULT_HEADERS = [
    "id",
    "name",
    "email",
    "amount",
    "created_at",
    "is_active",
    "category",
]


def run(argv: Optional[List[str]] = None, save_main_session: bool = True) -> None:
    """Run the batch pipeline."""
    setup_logging(level=logging.INFO)
    logger.info("Starting Dataflow batch pipeline")

    parser = argparse.ArgumentParser(description="Dataflow CSV to BigQuery Pipeline")
    _, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    custom_options = pipeline_options.view_as(DataflowBatchOptions)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    logger.info(f"Input: {custom_options.get_input_path()}")
    logger.info(f"Output table: {custom_options.get_bigquery_table()}")
    logger.info(f"Dead letter: {custom_options.get_dead_letter_path()}")

    schema_path = custom_options.schema_file
    field_types = get_field_types(schema_path)
    required_fields = [
        f for f in get_required_fields(schema_path) if not f.startswith("_")
    ]

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Step 1: Read CSV from GCS
        records = pipeline | "ReadCSV" >> ReadCSVFromGCS(
            file_path=custom_options.get_input_path(),
            headers=DEFAULT_HEADERS,
            delimiter=custom_options.delimiter,
            skip_header=custom_options.has_header,
        )

        # Step 2: Validate records
        validation_result = records | "ValidateRecords" >> ValidateRecords(
            required_fields=required_fields,
            field_types=field_types,
        )

        valid_records = validation_result[VALID_TAG]
        invalid_records = validation_result[INVALID_TAG]

        # Step 3: Transform valid records
        transformed_records = valid_records | "TransformRecords" >> TransformRecords(
            field_types=field_types,
            source_file=custom_options.get_input_path(),
        )

        # Step 4: Write to BigQuery
        _ = transformed_records | "WriteToBigQuery" >> WriteToBigQuery(
            table=custom_options.get_bigquery_table(),
            schema_path=schema_path,
        )

        # Step 5: Write invalid records to dead letter
        _ = invalid_records | "WriteToDeadLetter" >> WriteToDeadLetter(
            output_path=custom_options.get_dead_letter_path(),
        )

    logger.info("Pipeline completed successfully")


if __name__ == "__main__":
    run()
