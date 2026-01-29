"""Pipeline options for Dataflow job configuration."""

from apache_beam.options.pipeline_options import PipelineOptions


class DataflowBatchOptions(PipelineOptions):
    """Custom pipeline options for the CSV to BigQuery batch pipeline."""

    @classmethod
    def _add_argparse_args(cls, parser):
        # Input configuration
        parser.add_argument(
            "--input_bucket",
            type=str,
            required=True,
            help="GCS bucket name containing input CSV files",
        )
        parser.add_argument(
            "--input_file",
            type=str,
            required=True,
            help="Path to the input CSV file within the bucket",
        )
        parser.add_argument(
            "--delimiter",
            type=str,
            default=",",
            help="CSV delimiter character (default: ',')",
        )
        parser.add_argument(
            "--has_header",
            type=bool,
            default=True,
            help="Whether the CSV file has a header row (default: True)",
        )

        # Output configuration - BigQuery
        parser.add_argument(
            "--output_project",
            type=str,
            help="GCP project for BigQuery output (defaults to pipeline project)",
        )
        parser.add_argument(
            "--output_dataset",
            type=str,
            required=True,
            help="BigQuery dataset name for output table",
        )
        parser.add_argument(
            "--output_table",
            type=str,
            required=True,
            help="BigQuery table name for valid records",
        )

        # Dead letter configuration
        parser.add_argument(
            "--dead_letter_bucket",
            type=str,
            required=True,
            help="GCS bucket for dead letter records",
        )
        parser.add_argument(
            "--dead_letter_prefix",
            type=str,
            default="dead_letter",
            help="Prefix path within dead letter bucket",
        )

        # Schema configuration
        parser.add_argument(
            "--schema_file",
            type=str,
            default="schemas/bigquery_schema.json",
            help="Path to BigQuery schema JSON file",
        )

    def get_input_path(self) -> str:
        return f"gs://{self.input_bucket}/{self.input_file}"

    def get_dead_letter_path(self) -> str:
        return f"gs://{self.dead_letter_bucket}/{self.dead_letter_prefix}"

    def get_bigquery_table(self) -> str:
        project = self.output_project or self.project
        return f"{project}:{self.output_dataset}.{self.output_table}"
