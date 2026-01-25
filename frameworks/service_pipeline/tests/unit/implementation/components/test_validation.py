import pytest
from unittest.mock import Mock, patch
from frameworks.service_pipeline.implementation.components.validation import ValidationComponent


class TestValidationComponent:

    def test_init_default_config(self):
        """Test initialization with default configuration."""
        component = ValidationComponent()
        assert component.config == {}
        assert component.required_fields == []

    def test_init_with_required_fields(self):
        """Test initialization with required fields configuration."""
        config = {"required_fields": ["field1", "field2", "field3"]}
        component = ValidationComponent(config)
        assert component.config == config
        assert component.required_fields == ["field1", "field2", "field3"]

    def test_init_with_full_config(self):
        """Test initialization with full configuration."""
        config = {
            "required_fields": ["user_id", "data"],
            "strict_mode": True,
            "custom_param": "value"
        }
        component = ValidationComponent(config)
        assert component.required_fields == ["user_id", "data"]
        assert component.config["strict_mode"] is True

    @patch('frameworks.service_pipeline.implementation.components.validation.BaseComponent.execute')
    def test_execute_validation_passes_all_fields_present(self, mock_super_execute):
        """Test successful validation when all required fields are present."""
        config = {"required_fields": ["user_id", "data"]}
        component = ValidationComponent(config)

        context = {
            "user_id": "user123",
            "data": {"key": "value"},
            "service_id": "validation_service"
        }

        with patch.object(component, 'log_info') as mock_log_info:
            result = component.execute(context)

            # Verify parent execute was called
            mock_super_execute.assert_called_once_with(context)

            # Verify validation results
            assert result["validation_passed"] is True
            assert result["validation_errors"] == []
            assert result["validated_data"] == {"key": "value"}

            # Verify logging
            mock_log_info.assert_any_call("Validating data with 1 fields")
            mock_log_info.assert_any_call("Validation passed successfully")

    @patch('frameworks.service_pipeline.implementation.components.validation.BaseComponent.execute')
    def test_execute_validation_fails_missing_required_fields(self, mock_super_execute):
        """Test validation failure when required fields are missing."""
        config = {"required_fields": ["user_id", "data", "timestamp"]}
        component = ValidationComponent(config)

        context = {
            "user_id": "user123",
            # Missing 'data' and 'timestamp'
            "service_id": "validation_service"
        }

        with patch.object(component, 'log_warning') as mock_log_warning:
            with patch.object(component, 'log_error') as mock_log_error:
                result = component.execute(context)

                # Verify validation results
                assert result["validation_passed"] is False
                assert len(result["validation_errors"]) == 2
                assert "Missing required field: data" in result["validation_errors"]
                assert "Missing required field: timestamp" in result["validation_errors"]
                assert "validated_data" not in result

                # Verify logging
                mock_log_warning.assert_any_call("Validation: Missing required field 'data'")
                mock_log_warning.assert_any_call("Validation: Missing required field 'timestamp'")
                mock_log_error.assert_called_once_with("Validation failed with 2 error(s)")

    @patch('frameworks.service_pipeline.implementation.components.validation.BaseComponent.execute')
    def test_execute_validation_fails_invalid_data_type(self, mock_super_execute):
        """Test validation failure when data is not a dictionary."""
        component = ValidationComponent()

        context = {
            "data": "not_a_dictionary",  # Should be dict
            "service_id": "validation_service"
        }

        with patch.object(component, 'log_warning') as mock_log_warning:
            with patch.object(component, 'log_error') as mock_log_error:
                result = component.execute(context)

                # Verify validation results
                assert result["validation_passed"] is False
                assert "Data must be a dictionary" in result["validation_errors"]
                assert "validated_data" not in result

                # Verify logging
                mock_log_warning.assert_called_with("Validation: Data is not a dictionary")
                mock_log_error.assert_called_once_with("Validation failed with 1 error(s)")

    @patch('frameworks.service_pipeline.implementation.components.validation.BaseComponent.execute')
    def test_execute_validation_passes_no_data_field(self, mock_super_execute):
        """Test validation passes when no data field is present."""
        component = ValidationComponent()

        context = {
            "service_id": "validation_service",
            "user_id": "user123"
        }

        with patch.object(component, 'log_info') as mock_log_info:
            result = component.execute(context)

            # Verify validation results
            assert result["validation_passed"] is True
            assert result["validation_errors"] == []
            assert "validated_data" not in result

            # Verify logging
            mock_log_info.assert_called_once_with("Validation passed successfully")

    @patch('frameworks.service_pipeline.implementation.components.validation.BaseComponent.execute')
    def test_execute_validation_passes_with_valid_data(self, mock_super_execute):
        """Test validation passes with valid dictionary data."""
        component = ValidationComponent()

        test_data = {
            "field1": "value1",
            "field2": 42,
            "nested": {"key": "value"}
        }

        context = {
            "data": test_data,
            "service_id": "validation_service"
        }

        with patch.object(component, 'log_info') as mock_log_info:
            result = component.execute(context)

            # Verify validation results
            assert result["validation_passed"] is True
            assert result["validation_errors"] == []
            assert result["validated_data"] == test_data

            # Verify logging
            mock_log_info.assert_any_call("Validating data with 3 fields")
            mock_log_info.assert_any_call("Validation passed successfully")

    @patch('frameworks.service_pipeline.implementation.components.validation.BaseComponent.execute')
    def test_execute_mixed_validation_errors(self, mock_super_execute):
        """Test validation with both missing fields and invalid data type."""
        config = {"required_fields": ["user_id", "timestamp"]}
        component = ValidationComponent(config)

        context = {
            "data": ["not", "a", "dictionary"],  # Invalid type
            "user_id": "user123"
            # Missing 'timestamp'
        }

        with patch.object(component, 'log_warning') as mock_log_warning:
            with patch.object(component, 'log_error') as mock_log_error:
                result = component.execute(context)

                # Verify validation results
                assert result["validation_passed"] is False
                assert len(result["validation_errors"]) == 2
                assert "Missing required field: timestamp" in result["validation_errors"]
                assert "Data must be a dictionary" in result["validation_errors"]

    def test_component_inheritance(self):
        """Test that component properly inherits from BaseComponent."""
        from frameworks.service_pipeline.implementation.base_component import BaseComponent

        component = ValidationComponent()
        assert isinstance(component, BaseComponent)

    @patch('frameworks.service_pipeline.implementation.components.validation.BaseComponent.execute')
    def test_execute_preserves_original_context(self, mock_super_execute):
        """Test that original context is preserved and enhanced."""
        config = {"required_fields": ["user_id"]}
        component = ValidationComponent(config)

        original_context = {
            "user_id": "user123",
            "data": {"field": "value"},
            "existing_field": "preserved",
            "service_id": "test"
        }

        with patch.object(component, 'log_info'):
            result = component.execute(original_context)

            # Original fields should be preserved
            assert result["user_id"] == "user123"
            assert result["data"] == {"field": "value"}
            assert result["existing_field"] == "preserved"
            assert result["service_id"] == "test"

            # New validation fields should be added
            assert "validation_passed" in result
            assert "validation_errors" in result
            assert "validated_data" in result

    @patch('frameworks.service_pipeline.implementation.components.validation.BaseComponent.execute')
    def test_execute_empty_context(self, mock_super_execute):
        """Test validation with empty context."""
        component = ValidationComponent()
        context = {}

        with patch.object(component, 'log_info') as mock_log_info:
            result = component.execute(context)

            # Should pass validation with no requirements
            assert result["validation_passed"] is True
            assert result["validation_errors"] == []

    @patch('frameworks.service_pipeline.implementation.components.validation.BaseComponent.execute')
    def test_execute_required_fields_empty_list(self, mock_super_execute):
        """Test validation with empty required fields list."""
        config = {"required_fields": []}
        component = ValidationComponent(config)

        context = {"any_field": "any_value", "data": {"test": "data"}}

        with patch.object(component, 'log_info') as mock_log_info:
            result = component.execute(context)

            # Should pass validation with no requirements
            assert result["validation_passed"] is True
            assert result["validation_errors"] == []
            assert result["validated_data"] == {"test": "data"}

    @patch('frameworks.service_pipeline.implementation.components.validation.BaseComponent.execute', side_effect=Exception("Base component error"))
    def test_execute_parent_exception_propagation(self, mock_super_execute):
        """Test that exceptions from parent execute are propagated."""
        component = ValidationComponent()
        context = {"service_id": "test"}

        with pytest.raises(Exception, match="Base component error"):
            component.execute(context)

    @patch('frameworks.service_pipeline.implementation.components.validation.BaseComponent.execute')
    def test_execute_data_none_value(self, mock_super_execute):
        """Test validation when data field is None."""
        component = ValidationComponent()
        context = {"data": None}

        with patch.object(component, 'log_warning') as mock_log_warning:
            result = component.execute(context)

            # Should fail validation as None is not a dictionary
            assert result["validation_passed"] is False
            assert "Data must be a dictionary" in result["validation_errors"]

    @patch('frameworks.service_pipeline.implementation.components.validation.BaseComponent.execute')
    def test_execute_complex_data_validation(self, mock_super_execute):
        """Test validation with complex nested data structure."""
        component = ValidationComponent()

        complex_data = {
            "user": {
                "id": "user123",
                "profile": {
                    "name": "John Doe",
                    "settings": {"theme": "dark", "notifications": True}
                }
            },
            "request": {
                "type": "validation",
                "parameters": ["field1", "field2"],
                "timestamp": "2023-01-01T00:00:00Z"
            }
        }

        context = {"data": complex_data}

        with patch.object(component, 'log_info') as mock_log_info:
            result = component.execute(context)

            # Should handle complex data structures
            assert result["validation_passed"] is True
            assert result["validated_data"] == complex_data
            mock_log_info.assert_any_call("Validating data with 2 fields")
