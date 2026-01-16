import pytest
from unittest.mock import Mock, patch
from components.transformation import TransformationComponent


class TestTransformationComponent:

    def test_init_default_config(self):
        """Test initialization with default configuration."""
        component = TransformationComponent()
        assert component.config == {}
        assert component.transform_type == "uppercase"

    def test_init_with_transform_type(self):
        """Test initialization with specific transform type."""
        config = {"transform_type": "normalize"}
        component = TransformationComponent(config)
        assert component.transform_type == "normalize"

    def test_init_with_full_config(self):
        """Test initialization with full configuration."""
        config = {
            "transform_type": "uppercase",
            "preserve_case": True,
            "custom_param": "value"
        }
        component = TransformationComponent(config)
        assert component.transform_type == "uppercase"
        assert component.config["preserve_case"] is True

    @patch('components.transformation.BaseComponent.execute')
    def test_execute_uppercase_transform_string_data(self, mock_super_execute):
        """Test uppercase transformation with string data."""
        component = TransformationComponent({"transform_type": "uppercase"})

        context = {
            "data": {"text": "hello world", "name": "john doe"},
            "service_id": "transform_service"
        }

        with patch.object(component, 'log_info') as mock_log_info:
            result = component.execute(context)

            # Verify parent execute was called
            mock_super_execute.assert_called_once_with(context)

            # Verify transformation results
            expected_transformed = {"text": "HELLO WORLD", "name": "JOHN DOE"}
            assert result["transformed_data"] == expected_transformed
            assert result["transform_type"] == "uppercase"
            assert result["processed"] == expected_transformed
            assert result["original_keys"] == ["text", "name"]

            # Verify logging
            mock_log_info.assert_any_call("Starting uppercase transformation")
            mock_log_info.assert_any_call("Applied uppercase transformation")
            mock_log_info.assert_any_call("Transformed 2 fields into 2 fields")

    @patch('components.transformation.BaseComponent.execute')
    def test_execute_uppercase_transform_mixed_data(self, mock_super_execute):
        """Test uppercase transformation with mixed data types."""
        component = TransformationComponent({"transform_type": "uppercase"})

        context = {
            "data": {
                "text": "hello",
                "number": 42,
                "boolean": True,
                "nested": {"inner": "world"}
            }
        }

        with patch.object(component, 'log_info') as mock_log_info:
            result = component.execute(context)

            expected_transformed = {
                "text": "HELLO",
                "number": 42,
                "boolean": True,
                "nested": {"inner": "WORLD"}
            }
            assert result["transformed_data"] == expected_transformed

    @patch('components.transformation.BaseComponent.execute')
    def test_execute_uppercase_transform_list_data(self, mock_super_execute):
        """Test uppercase transformation with list data."""
        component = TransformationComponent({"transform_type": "uppercase"})

        context = {
            "data": {"items": ["hello", "world", 123]}
        }

        with patch.object(component, 'log_info'):
            result = component.execute(context)

            expected_transformed = {"items": ["HELLO", "WORLD", 123]}
            assert result["transformed_data"] == expected_transformed

    @patch('components.transformation.BaseComponent.execute')
    def test_execute_normalize_transform(self, mock_super_execute):
        """Test normalize transformation."""
        component = TransformationComponent({"transform_type": "normalize"})

        context = {
            "data": {
                "user": {"id": "123", "name": "john"},
                "settings": {"theme": "dark", "lang": "en"},
                "status": "active"
            }
        }

        with patch.object(component, 'log_info') as mock_log_info:
            result = component.execute(context)

            expected_transformed = {
                "user_id": "123",
                "user_name": "john",
                "settings_theme": "dark",
                "settings_lang": "en",
                "status": "active"
            }
            assert result["transformed_data"] == expected_transformed
            assert result["transform_type"] == "normalize"

            mock_log_info.assert_any_call("Starting normalize transformation")
            mock_log_info.assert_any_call("Applied normalization transformation")

    @patch('components.transformation.BaseComponent.execute')
    def test_execute_unknown_transform_type(self, mock_super_execute):
        """Test execution with unknown transform type."""
        component = TransformationComponent({"transform_type": "unknown"})

        context = {"data": {"key": "value"}}

        with patch.object(component, 'log_info') as mock_log_info:
            result = component.execute(context)

            # Should return original data unchanged
            assert result["transformed_data"] == {"key": "value"}
            assert result["transform_type"] == "unknown"

            mock_log_info.assert_any_call("No transformation applied (unknown type)")

    @patch('components.transformation.BaseComponent.execute')
    def test_execute_with_validated_data(self, mock_super_execute):
        """Test execution using validated_data instead of data."""
        component = TransformationComponent({"transform_type": "uppercase"})

        context = {
            "data": {"original": "data"},
            "validated_data": {"validated": "content"},
            "service_id": "transform_service"
        }

        with patch.object(component, 'log_info'):
            result = component.execute(context)

            # Should use validated_data, not data
            expected_transformed = {"validated": "CONTENT"}
            assert result["transformed_data"] == expected_transformed

    @patch('components.transformation.BaseComponent.execute')
    def test_execute_no_data(self, mock_super_execute):
        """Test execution when no data is present."""
        component = TransformationComponent({"transform_type": "uppercase"})

        context = {"service_id": "transform_service"}

        with patch.object(component, 'log_info') as mock_log_info:
            result = component.execute(context)

            # Should handle empty data gracefully
            assert result["transformed_data"] == {}
            assert result["original_keys"] == []

    @patch('components.transformation.BaseComponent.execute')
    def test_execute_preserves_original_context(self, mock_super_execute):
        """Test that original context fields are preserved."""
        component = TransformationComponent()

        context = {
            "data": {"text": "hello"},
            "service_id": "test_service",
            "request_id": "req123",
            "existing_field": "preserved"
        }

        with patch.object(component, 'log_info'):
            result = component.execute(context)

            # Original fields should be preserved
            assert result["data"] == {"text": "hello"}
            assert result["service_id"] == "test_service"
            assert result["request_id"] == "req123"
            assert result["existing_field"] == "preserved"

            # New fields should be added
            assert "transformed_data" in result
            assert "transform_type" in result
            assert "processed" in result
            assert "original_keys" in result

    def test_component_inheritance(self):
        """Test that component properly inherits from BaseComponent."""
        from framework.base_component import BaseComponent

        component = TransformationComponent()
        assert isinstance(component, BaseComponent)

    def test_uppercase_transform_edge_cases(self):
        """Test _uppercase_transform with edge cases."""
        component = TransformationComponent()

        # Test None
        assert component._uppercase_transform(None) is None

        # Test empty string
        assert component._uppercase_transform("") == ""

        # Test number
        assert component._uppercase_transform(42) == 42

        # Test boolean
        assert component._uppercase_transform(True) is True

        # Test empty dict
        assert component._uppercase_transform({}) == {}

        # Test empty list
        assert component._uppercase_transform([]) == []

        # Test nested structure
        nested = {
            "level1": {
                "level2": ["hello", "world", 123]
            }
        }
        expected = {
            "level1": {
                "level2": ["HELLO", "WORLD", 123]
            }
        }
        assert component._uppercase_transform(nested) == expected

    def test_normalize_transform_edge_cases(self):
        """Test _normalize_transform with edge cases."""
        component = TransformationComponent()

        # Test None
        assert component._normalize_transform(None) is None

        # Test string
        assert component._normalize_transform("hello") == "hello"

        # Test number
        assert component._normalize_transform(42) == 42

        # Test empty dict
        assert component._normalize_transform({}) == {}

        # Test flat dict
        flat_dict = {"key1": "value1", "key2": "value2"}
        assert component._normalize_transform(flat_dict) == flat_dict

        # Test deeply nested dict
        nested = {
            "user": {
                "profile": {
                    "name": "john"
                }
            },
            "simple": "value"
        }
        expected = {
            "user_profile": {"name": "john"},
            "simple": "value"
        }
        assert component._normalize_transform(nested) == expected

    @patch('components.transformation.BaseComponent.execute')
    def test_execute_non_dict_data(self, mock_super_execute):
        """Test execution when data is not a dictionary."""
        component = TransformationComponent({"transform_type": "uppercase"})

        context = {"data": "simple string"}

        with patch.object(component, 'log_info') as mock_log_info:
            result = component.execute(context)

            # Should transform the string directly
            assert result["transformed_data"] == "SIMPLE STRING"
            assert result["original_keys"] == []  # Not a dict, so no keys

    @patch('components.transformation.BaseComponent.execute', side_effect=Exception("Base component error"))
    def test_execute_parent_exception_propagation(self, mock_super_execute):
        """Test that exceptions from parent execute are propagated."""
        component = TransformationComponent()
        context = {"service_id": "test"}

        with pytest.raises(Exception, match="Base component error"):
            component.execute(context)

    @patch('components.transformation.BaseComponent.execute')
    def test_execute_complex_nested_structure(self, mock_super_execute):
        """Test execution with complex nested data structure."""
        component = TransformationComponent({"transform_type": "uppercase"})

        complex_data = {
            "users": [
                {"name": "alice", "email": "alice@example.com"},
                {"name": "bob", "email": "bob@example.com"}
            ],
            "metadata": {
                "version": "1.0",
                "tags": ["production", "api"],
                "config": {
                    "debug": False,
                    "timeout": 30
                }
            }
        }

        context = {"data": complex_data}

        with patch.object(component, 'log_info'):
            result = component.execute(context)

            # Verify complex transformation
            transformed = result["transformed_data"]
            assert transformed["users"][0]["name"] == "ALICE"
            assert transformed["users"][0]["email"] == "ALICE@EXAMPLE.COM"
            assert transformed["metadata"]["version"] == "1.0"
            assert transformed["metadata"]["tags"] == ["PRODUCTION", "API"]
            assert transformed["metadata"]["config"]["debug"] is False
            assert transformed["metadata"]["config"]["timeout"] == 30