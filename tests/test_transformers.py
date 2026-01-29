"""Unit tests for the transformers module."""

import pytest

from pipeline.transforms.transformers import TransformRecord, TransformRecords


class TestTransformRecord:
    """Tests for the TransformRecord DoFn."""

    def setup_method(self):
        self.field_types = {
            "id": "INTEGER",
            "name": "STRING",
            "email": "STRING",
            "amount": "FLOAT",
            "is_active": "BOOLEAN",
            "created_at": "TIMESTAMP",
        }
        self.transformer = TransformRecord(
            field_types=self.field_types,
            source_file="gs://test-bucket/test.csv",
        )

    def test_integer_conversion(self):
        record = {"id": "123", "name": "Test"}
        results = list(self.transformer.process(record))
        assert len(results) == 1
        assert results[0]["id"] == 123
        assert isinstance(results[0]["id"], int)

    def test_integer_from_float_string(self):
        record = {"id": "123.0", "name": "Test"}
        results = list(self.transformer.process(record))
        assert results[0]["id"] == 123

    def test_float_conversion(self):
        record = {"amount": "99.99"}
        results = list(self.transformer.process(record))
        assert results[0]["amount"] == 99.99
        assert isinstance(results[0]["amount"], float)

    def test_boolean_conversion_true(self):
        for val in ["true", "True", "1", "yes"]:
            record = {"is_active": val}
            results = list(self.transformer.process(record))
            assert results[0]["is_active"] is True

    def test_boolean_conversion_false(self):
        for val in ["false", "False", "0", "no"]:
            record = {"is_active": val}
            results = list(self.transformer.process(record))
            assert results[0]["is_active"] is False

    def test_timestamp_conversion(self):
        record = {"created_at": "2024-01-15 10:30:00"}
        results = list(self.transformer.process(record))
        assert "2024-01-15" in results[0]["created_at"]

    def test_string_normalization(self):
        record = {"name": "  John Doe  ", "email": "  john@example.com  "}
        results = list(self.transformer.process(record))
        assert results[0]["name"] == "John Doe"
        assert results[0]["email"] == "john@example.com"

    def test_null_values_preserved(self):
        record = {"id": "123", "name": None, "amount": None}
        results = list(self.transformer.process(record))
        assert results[0]["id"] == 123
        assert results[0]["name"] is None
        assert results[0]["amount"] is None

    def test_audit_fields_added(self):
        record = {"id": "123"}
        results = list(self.transformer.process(record))
        assert "_load_timestamp" in results[0]
        assert "_source_file" in results[0]
        assert results[0]["_source_file"] == "gs://test-bucket/test.csv"


class TestTransformRecords:
    """Tests for the TransformRecords PTransform."""

    def test_transform_initialization(self):
        transform = TransformRecords(
            field_types={"id": "INTEGER"},
            source_file="gs://bucket/file.csv",
        )
        assert transform.field_types == {"id": "INTEGER"}
        assert transform.source_file == "gs://bucket/file.csv"
