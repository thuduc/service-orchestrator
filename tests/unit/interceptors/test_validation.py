import pytest
import json
from unittest.mock import Mock, patch
from interceptors.validation import ValidationInterceptor, ValidationError


class TestValidationInterceptor:

    def test_init_default_config(self):
        """Test initialization with default configuration."""
        interceptor = ValidationInterceptor()

        assert interceptor.config == {}
        assert interceptor.validate_request is True
        assert interceptor.validate_response is False
        assert interceptor.strict_mode is False
        assert interceptor.required_fields == ['service_id']
        assert interceptor.max_payload_size == 1024 * 1024

    def test_init_custom_config(self):
        """Test initialization with custom configuration."""
        config = {
            "validate_request": False,
            "validate_response": True,
            "strict_mode": True,
            "required_fields": ["service_id", "user_id", "timestamp"],
            "max_payload_size": 512 * 1024
        }

        interceptor = ValidationInterceptor(config)

        assert interceptor.validate_request is False
        assert interceptor.validate_response is True
        assert interceptor.strict_mode is True
        assert interceptor.required_fields == ["service_id", "user_id", "timestamp"]
        assert interceptor.max_payload_size == 512 * 1024

    def test_before_valid_request_non_strict(self):
        """Test before() with valid request in non-strict mode."""
        interceptor = ValidationInterceptor({
            "validate_request": True,
            "strict_mode": False
        })

        context = {
            "service_id": "test_service",
            "request_id": "req_123",
            "data": {"key": "value"}
        }

        result = interceptor.before(context)

        assert result == context

    def test_before_invalid_request_non_strict(self):
        """Test before() with invalid request in non-strict mode."""
        interceptor = ValidationInterceptor({
            "validate_request": True,
            "strict_mode": False,
            "required_fields": ["service_id", "user_id"]
        })

        context = {
            "service_id": "test_service"
            # Missing user_id
        }

        with patch.object(interceptor.logger, 'warning') as mock_warning:
            result = interceptor.before(context)

            # Should still return context despite warnings
            assert result == context
            mock_warning.assert_called()

    def test_before_invalid_request_strict_mode(self):
        """Test before() with invalid request in strict mode."""
        interceptor = ValidationInterceptor({
            "validate_request": True,
            "strict_mode": True,
            "required_fields": ["service_id", "user_id"]
        })

        context = {
            "service_id": "test_service"
            # Missing user_id
        }

        with pytest.raises(ValidationError, match="Request validation failed"):
            interceptor.before(context)

    def test_after_valid_response(self):
        """Test after() with valid response."""
        interceptor = ValidationInterceptor({
            "validate_response": True,
            "strict_mode": False
        })

        context = {"service_id": "test_service"}
        result = {"status": "success", "data": {"result": "value"}}

        returned_result = interceptor.after(context, result)

        assert returned_result == result

    def test_after_invalid_response_strict(self):
        """Test after() with invalid response in strict mode."""
        interceptor = ValidationInterceptor({
            "validate_response": True,
            "strict_mode": True
        })

        context = {"service_id": "test_service"}
        result = "invalid_response_not_dict"

        with pytest.raises(ValidationError, match="Response validation failed"):
            interceptor.after(context, result)

    def test_validate_request_missing_required_fields(self):
        """Test request validation with missing required fields."""
        interceptor = ValidationInterceptor({
            "required_fields": ["service_id", "user_id", "timestamp"]
        })

        context = {"service_id": "test_service"}

        errors = interceptor._validate_request(context)

        assert len(errors) == 2
        assert "Missing required field: user_id" in errors
        assert "Missing required field: timestamp" in errors

    def test_validate_request_invalid_service_id(self):
        """Test request validation with invalid service_id."""
        interceptor = ValidationInterceptor()

        test_cases = [
            {"service_id": ""},  # Empty string
            {"service_id": 123},  # Not a string
            {"service_id": None},  # None value
            {"service_id": "invalid service!"},  # Invalid characters
        ]

        for context in test_cases:
            errors = interceptor._validate_request(context)
            assert any("Invalid service_id" in error for error in errors)

    def test_validate_request_invalid_request_id(self):
        """Test request validation with invalid request_id."""
        interceptor = ValidationInterceptor()

        test_cases = [
            {"service_id": "valid", "request_id": ""},  # Empty string
            {"service_id": "valid", "request_id": 456},  # Not a string
            {"service_id": "valid", "request_id": None},  # None value
        ]

        for context in test_cases:
            errors = interceptor._validate_request(context)
            assert any("Invalid request_id" in error for error in errors)

    def test_validate_request_payload_too_large(self):
        """Test request validation with oversized payload."""
        interceptor = ValidationInterceptor({"max_payload_size": 100})

        large_data = "x" * 200  # Create data larger than limit
        context = {
            "service_id": "test_service",
            "data": large_data
        }

        errors = interceptor._validate_request(context)

        assert any("Payload too large" in error for error in errors)

    def test_validate_request_invalid_data_type(self):
        """Test request validation with invalid data type."""
        interceptor = ValidationInterceptor()

        class CustomObject:
            pass

        context = {
            "service_id": "test_service",
            "data": CustomObject()  # Non-serializable object
        }

        errors = interceptor._validate_request(context)

        assert any("Invalid data type" in error for error in errors)

    def test_validate_response_not_dict(self):
        """Test response validation when response is not a dictionary."""
        interceptor = ValidationInterceptor()

        test_responses = ["string", 123, ["list"], None]

        for response in test_responses:
            errors = interceptor._validate_response(response)
            assert "Response must be a dictionary" in errors

    def test_validate_response_too_large(self):
        """Test response validation with oversized response."""
        interceptor = ValidationInterceptor({"max_payload_size": 100})

        large_response = {"data": "x" * 500}  # Larger than 2 * max_payload_size

        errors = interceptor._validate_response(large_response)

        assert any("Response too large" in error for error in errors)

    def test_validate_response_error_without_details(self):
        """Test response validation for error responses missing required fields."""
        interceptor = ValidationInterceptor()

        # Error response without error_code
        response1 = {"error": True}
        errors1 = interceptor._validate_response(response1)
        assert "Response with error should include error_code" in errors1
        assert "Response with error should include error_message" in errors1

        # Error response with only error_code
        response2 = {"error": True, "error_code": "E001"}
        errors2 = interceptor._validate_response(response2)
        assert "Response with error should include error_message" in errors2
        assert "Response with error should include error_code" not in errors2

    def test_is_valid_service_id(self):
        """Test service ID format validation."""
        interceptor = ValidationInterceptor()

        # Valid service IDs
        valid_ids = [
            "service1",
            "my-service",
            "service_name",
            "Service123",
            "service-with-dashes",
            "service_with_underscores",
            "123service"
        ]

        for service_id in valid_ids:
            assert interceptor._is_valid_service_id(service_id) is True

        # Invalid service IDs
        invalid_ids = [
            "service with spaces",
            "service!",
            "service@domain",
            "service.name",
            "service#1",
            "service/path",
            "",
        ]

        for service_id in invalid_ids:
            assert interceptor._is_valid_service_id(service_id) is False

    def test_estimate_size_json_serializable(self):
        """Test size estimation for JSON-serializable objects."""
        interceptor = ValidationInterceptor()

        test_objects = [
            "hello",
            123,
            {"key": "value"},
            ["item1", "item2"],
            {"nested": {"data": [1, 2, 3]}}
        ]

        for obj in test_objects:
            size = interceptor._estimate_size(obj)
            expected_size = len(json.dumps(obj).encode('utf-8'))
            assert size == expected_size

    def test_estimate_size_non_json_serializable(self):
        """Test size estimation for non-JSON-serializable objects."""
        interceptor = ValidationInterceptor()

        class CustomObject:
            def __str__(self):
                return "CustomObject instance"

        obj = CustomObject()
        size = interceptor._estimate_size(obj)
        expected_size = len(str(obj).encode('utf-8'))
        assert size == expected_size

    def test_interceptor_inheritance(self):
        """Test that interceptor properly inherits from Interceptor."""
        from framework.interceptor import Interceptor

        interceptor = ValidationInterceptor()
        assert isinstance(interceptor, Interceptor)

    def test_before_no_validation(self):
        """Test before() with all validation disabled."""
        interceptor = ValidationInterceptor({
            "validate_request": False
        })

        context = {}  # Empty context (would normally fail validation)

        result = interceptor.before(context)

        # Should pass without validation
        assert result == context

    def test_after_no_validation(self):
        """Test after() with all validation disabled."""
        interceptor = ValidationInterceptor({
            "validate_response": False
        })

        context = {"service_id": "test"}
        result = "not_a_dict"

        returned_result = interceptor.after(context, result)

        # Should pass without validation
        assert returned_result == result

    def test_validation_error_exception(self):
        """Test ValidationError exception properties."""
        error_message = "Test validation error"
        error = ValidationError(error_message)

        assert str(error) == error_message
        assert isinstance(error, Exception)

    def test_complex_validation_scenario(self):
        """Test complex validation with multiple requirements."""
        config = {
            "validate_request": True,
            "validate_response": True,
            "strict_mode": True,
            "required_fields": ["service_id", "user_id", "session_id"],
            "max_payload_size": 1000
        }

        interceptor = ValidationInterceptor(config)

        # Valid request and response
        valid_context = {
            "service_id": "complex-service",
            "user_id": "user_123",
            "session_id": "session_abc",
            "data": {"small": "payload"}
        }

        valid_response = {
            "status": "success",
            "result": {"processed": True}
        }

        result_context = interceptor.before(valid_context)
        returned_result = interceptor.after(result_context, valid_response)

        assert result_context == valid_context
        assert returned_result == valid_response

    def test_before_preserves_context(self):
        """Test that before() doesn't modify the original context structure."""
        interceptor = ValidationInterceptor()

        original_context = {
            "service_id": "preserve_test",
            "data": {"original": "data"},
            "metadata": {"preserve": True}
        }

        # Create a copy to compare
        context_copy = original_context.copy()

        result = interceptor.before(original_context)

        # Context should remain unchanged
        assert original_context == context_copy
        assert result == original_context
