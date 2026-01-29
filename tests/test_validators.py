"""Unit tests for the validators module."""

import pytest

from pipeline.transforms.validators import ValidateRecord, ValidateRecords, VALID_TAG, INVALID_TAG


class TestValidateRecord:
    """Tests for the ValidateRecord DoFn."""

    def setup_method(self):
        self.required_fields = ["id", "name", "email"]
        self.field_types = {
            "id": "INTEGER",
            "name": "STRING",
            "email": "STRING",
            "amount": "FLOAT",
            "is_active": "BOOLEAN",
        }
        self.validator = ValidateRecord(
            required_fields=self.required_fields,
            field_types=self.field_types,
        )

    def test_valid_record_passes(self):
        record = {
            "id": "123",
            "name": "John Doe",
            "email": "john@example.com",
            "amount": "99.99",
            "is_active": "true",
        }
        results = list(self.validator.process(record))
        assert len(results) == 1
        assert results[0].tag == VALID_TAG

    def test_missing_required_field_fails(self):
        record = {"id": "123", "name": "John Doe"}
        results = list(self.validator.process(record))
        assert len(results) == 1
        assert results[0].tag == INVALID_TAG
        assert "Missing required field: email" in results[0].value["error_message"]

    def test_null_required_field_fails(self):
        record = {"id": "123", "name": None, "email": "john@example.com"}
        results = list(self.validator.process(record))
        assert len(results) == 1
        assert results[0].tag == INVALID_TAG

    def test_invalid_integer_type_fails(self):
        record = {"id": "not_a_number", "name": "John Doe", "email": "john@example.com"}
        results = list(self.validator.process(record))
        assert len(results) == 1
        assert results[0].tag == INVALID_TAG
        assert "cannot be converted to INTEGER" in results[0].value["error_message"]

    def test_invalid_email_format_fails(self):
        record = {"id": "123", "name": "John Doe", "email": "not_an_email"}
        results = list(self.validator.process(record))
        assert len(results) == 1
        assert results[0].tag == INVALID_TAG
        assert "Invalid email format" in results[0].value["error_message"]

    def test_valid_email_formats(self):
        valid_emails = [
            "user@example.com",
            "user.name@example.com",
            "user+tag@example.com",
        ]
        for email in valid_emails:
            record = {"id": "123", "name": "John Doe", "email": email}
            results = list(self.validator.process(record))
            assert results[0].tag == VALID_TAG, f"Email {email} should be valid"

    def test_parse_error_record_sent_to_invalid(self):
        record = {
            "_parse_error": True,
            "_error_message": "Failed to parse CSV line",
            "_raw_line": "malformed,data",
        }
        results = list(self.validator.process(record))
        assert len(results) == 1
        assert results[0].tag == INVALID_TAG
        assert results[0].value["error_type"] == "PARSE_ERROR"

    def test_nullable_fields_with_none(self):
        record = {
            "id": "123",
            "name": "John Doe",
            "email": "john@example.com",
            "amount": None,
            "is_active": None,
        }
        results = list(self.validator.process(record))
        assert len(results) == 1
        assert results[0].tag == VALID_TAG

    def test_invalid_boolean_fails(self):
        record = {
            "id": "123",
            "name": "John Doe",
            "email": "john@example.com",
            "is_active": "maybe",
        }
        results = list(self.validator.process(record))
        assert len(results) == 1
        assert results[0].tag == INVALID_TAG
        assert "invalid boolean value" in results[0].value["error_message"]


class TestValidateRecords:
    """Tests for the ValidateRecords PTransform."""

    def test_transform_initialization(self):
        transform = ValidateRecords(
            required_fields=["id", "name"],
            field_types={"id": "INTEGER"},
            max_field_lengths={"name": 100},
        )
        assert transform.required_fields == ["id", "name"]
        assert transform.field_types == {"id": "INTEGER"}
