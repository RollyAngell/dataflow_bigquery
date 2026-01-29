"""Integration tests for the pipeline."""

import pytest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_not_empty

from pipeline.transforms.readers import ParseCSVLine
from pipeline.transforms.validators import ValidateRecords, VALID_TAG, INVALID_TAG
from pipeline.transforms.transformers import TransformRecords


class TestParseCSVLine:
    """Tests for CSV parsing."""

    def test_parse_simple_csv_line(self):
        headers = ["id", "name", "email"]
        parser = ParseCSVLine(headers=headers, delimiter=",")
        line = "1,John Doe,john@example.com"
        results = list(parser.process(line))
        assert len(results) == 1
        assert results[0]["id"] == "1"
        assert results[0]["name"] == "John Doe"
        assert results[0]["email"] == "john@example.com"

    def test_parse_csv_with_quotes(self):
        headers = ["id", "name", "description"]
        parser = ParseCSVLine(headers=headers, delimiter=",")
        line = '1,"John, Jr.",Description'
        results = list(parser.process(line))
        assert results[0]["name"] == "John, Jr."

    def test_parse_csv_with_null_values(self):
        headers = ["id", "name", "email"]
        parser = ParseCSVLine(headers=headers, delimiter=",", null_values=["", "NULL"])
        line = "1,NULL,"
        results = list(parser.process(line))
        assert results[0]["id"] == "1"
        assert results[0]["name"] is None
        assert results[0]["email"] is None

    def test_parse_csv_with_semicolon(self):
        headers = ["id", "name", "email"]
        parser = ParseCSVLine(headers=headers, delimiter=";")
        line = "1;John Doe;john@example.com"
        results = list(parser.process(line))
        assert results[0]["name"] == "John Doe"


class TestPipelineIntegration:
    """Integration tests using Beam TestPipeline."""

    def test_validate_valid_records(self):
        with TestPipeline() as p:
            input_records = [
                {"id": "1", "name": "John", "email": "john@test.com"},
                {"id": "2", "name": "Jane", "email": "jane@test.com"},
            ]
            records = p | beam.Create(input_records)
            validation_result = records | ValidateRecords(
                required_fields=["id", "name", "email"],
                field_types={"id": "INTEGER"},
            )
            valid = validation_result[VALID_TAG]
            assert_that(valid, equal_to(input_records))

    def test_invalid_records_to_dead_letter(self):
        with TestPipeline() as p:
            input_records = [
                {"id": "1", "name": "John", "email": "john@test.com"},
                {"id": "2", "name": None, "email": "jane@test.com"},
            ]
            records = p | beam.Create(input_records)
            validation_result = records | ValidateRecords(
                required_fields=["id", "name", "email"],
            )
            valid = validation_result[VALID_TAG]
            invalid = validation_result[INVALID_TAG]

            assert_that(
                valid,
                equal_to([{"id": "1", "name": "John", "email": "john@test.com"}]),
            )
            assert_that(invalid, is_not_empty())

    def test_transform_converts_types(self):
        with TestPipeline() as p:
            input_records = [{"id": "123", "name": "John", "amount": "99.99"}]
            records = p | beam.Create(input_records)
            transformed = records | TransformRecords(
                field_types={"id": "INTEGER", "name": "STRING", "amount": "FLOAT"},
                source_file="test.csv",
            )

            def extract_data(record):
                return {"id": record["id"], "name": record["name"], "amount": record["amount"]}

            data_only = transformed | beam.Map(extract_data)
            assert_that(data_only, equal_to([{"id": 123, "name": "John", "amount": 99.99}]))

    def test_mixed_records_flow(self):
        with TestPipeline() as p:
            input_records = [
                {"id": "1", "name": "Alice", "email": "alice@test.com"},
                {"id": "2", "name": "Bob", "email": "bob@test.com"},
                {"id": "3", "name": None, "email": "charlie@test.com"},
                {"id": "not_int", "name": "Dave", "email": "dave@test.com"},
            ]
            records = p | beam.Create(input_records)
            validation_result = records | ValidateRecords(
                required_fields=["id", "name", "email"],
                field_types={"id": "INTEGER"},
            )
            valid = validation_result[VALID_TAG]
            invalid = validation_result[INVALID_TAG]

            valid_count = valid | "CountValid" >> beam.combiners.Count.Globally()
            invalid_count = invalid | "CountInvalid" >> beam.combiners.Count.Globally()

            assert_that(valid_count, equal_to([2]), label="ValidCount")
            assert_that(invalid_count, equal_to([2]), label="InvalidCount")
