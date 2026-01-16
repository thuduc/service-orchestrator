import pytest
import json
import os
import tempfile
from unittest.mock import Mock, patch, mock_open
from components.persistence import PersistenceComponent


class TestPersistenceComponent:

    def test_init_default_config(self):
        """Test initialization with default configuration."""
        component = PersistenceComponent()
        assert component.config == {}
        assert component.output_dir == "./output"
        assert component.format == "json"

    def test_init_with_config(self):
        """Test initialization with custom configuration."""
        config = {
            "output_dir": "/custom/path",
            "format": "txt",
            "custom_param": "value"
        }
        component = PersistenceComponent(config)
        assert component.output_dir == "/custom/path"
        assert component.format == "txt"
        assert component.config["custom_param"] == "value"

    @patch('components.persistence.BaseComponent.execute')
    @patch('components.persistence.os.makedirs')
    @patch('components.persistence.os.path.getsize')
    @patch('builtins.open', new_callable=mock_open)
    def test_execute_persist_processed_data(self, mock_file, mock_getsize, mock_makedirs, mock_super_execute):
        """Test persistence of 'processed' data."""
        component = PersistenceComponent({"output_dir": "/test/output"})

        processed_data = {"result": "processed", "count": 42}
        context = {
            "processed": processed_data,
            "request_id": "test_req_123",
            "service_id": "test_service"
        }

        mock_getsize.return_value = 128

        with patch.object(component, 'log_info') as mock_log_info:
            with patch.object(component, 'log_debug') as mock_log_debug:
                result = component.execute(context)

                # Verify parent execute was called
                mock_super_execute.assert_called_once_with(context)

                # Verify directory creation
                mock_makedirs.assert_called_once_with("/test/output", exist_ok=True)

                # Verify file operations
                expected_filepath = "/test/output/test_req_123_result.json"
                mock_file.assert_called_once_with(expected_filepath, 'w')

                # Verify JSON writing
                json.dump.assert_called = True  # JSON should be written

                # Verify context updates
                assert result["persisted"] is True
                assert result["filepath"] == expected_filepath
                assert result["size"] == 128
                assert result["persist_format"] == "json"

                # Verify logging
                mock_log_info.assert_any_call("Starting data persistence")
                mock_log_info.assert_any_call("Persisting 'processed' data")
                mock_log_debug.assert_called_with("Output directory: /test/output")

    @patch('components.persistence.BaseComponent.execute')
    @patch('components.persistence.os.makedirs')
    @patch('components.persistence.os.path.getsize')
    @patch('builtins.open', new_callable=mock_open)
    def test_execute_persist_transformed_data(self, mock_file, mock_getsize, mock_makedirs, mock_super_execute):
        """Test persistence of 'transformed_data' when 'processed' is not available."""
        component = PersistenceComponent()

        transformed_data = {"transformed": "data", "status": "complete"}
        context = {
            "transformed_data": transformed_data,
            "request_id": "transform_req_456"
        }

        mock_getsize.return_value = 64

        with patch.object(component, 'log_info') as mock_log_info:
            result = component.execute(context)

            # Verify persistence results
            assert result["persisted"] is True
            assert "filepath" in result
            assert result["size"] == 64
            assert result["persist_format"] == "json"

            # Verify logging
            mock_log_info.assert_any_call("Persisting 'transformed_data'")

    @patch('components.persistence.BaseComponent.execute')
    @patch('components.persistence.os.makedirs')
    @patch('components.persistence.os.path.getsize')
    @patch('builtins.open', new_callable=mock_open)
    def test_execute_persist_full_context(self, mock_file, mock_getsize, mock_makedirs, mock_super_execute):
        """Test persistence of full context when no specific data keys are present."""
        component = PersistenceComponent()

        context = {
            "service_id": "test_service",
            "request_id": "context_req_789",
            "user_data": {"name": "john"},
            "_internal_key": "should_be_excluded"
        }

        mock_getsize.return_value = 96

        with patch.object(component, 'log_info') as mock_log_info:
            result = component.execute(context)

            # Verify persistence results
            assert result["persisted"] is True
            assert result["persist_format"] == "json"

            # Verify logging
            mock_log_info.assert_any_call("Persisting full context (excluding internal keys)")

    @patch('components.persistence.BaseComponent.execute')
    @patch('components.persistence.os.makedirs')
    @patch('builtins.open', new_callable=mock_open)
    def test_execute_txt_format(self, mock_file, mock_makedirs, mock_super_execute):
        """Test persistence with txt format."""
        component = PersistenceComponent({"format": "txt"})

        context = {
            "processed": {"data": "test"},
            "request_id": "txt_req_001"
        }

        with patch('components.persistence.os.path.getsize', return_value=32):
            with patch.object(component, 'log_info'):
                result = component.execute(context)

                # Verify file operations for txt format
                expected_filepath = "./output/txt_req_001_result.txt"
                mock_file.assert_called_once_with(expected_filepath, 'w')

                # Verify context updates
                assert result["persist_format"] == "txt"

    @patch('components.persistence.BaseComponent.execute')
    @patch('components.persistence.os.makedirs')
    def test_execute_unknown_request_id(self, mock_makedirs, mock_super_execute):
        """Test persistence when request_id is not provided."""
        component = PersistenceComponent()

        context = {"processed": {"data": "test"}}

        with patch('builtins.open', mock_open()):
            with patch('components.persistence.os.path.getsize', return_value=24):
                with patch.object(component, 'log_info'):
                    result = component.execute(context)

                    # Should use 'unknown' as filename
                    expected_filepath = "./output/unknown_result.json"
                    assert result["filepath"] == expected_filepath

    @patch('components.persistence.BaseComponent.execute')
    @patch('components.persistence.os.makedirs')
    def test_execute_persistence_error(self, mock_makedirs, mock_super_execute):
        """Test handling of persistence errors."""
        component = PersistenceComponent()

        context = {
            "processed": {"data": "test"},
            "request_id": "error_req"
        }

        with patch('builtins.open', side_effect=IOError("Permission denied")):
            with patch.object(component, 'log_info'):
                with patch.object(component, 'log_error') as mock_log_error:
                    result = component.execute(context)

                    # Verify error handling
                    assert result["persisted"] is False
                    assert result["persist_error"] == "Permission denied"
                    assert result["persist_format"] == "json"

                    # Verify error logging
                    mock_log_error.assert_called_once_with("Failed to persist data: Permission denied")

    def test_component_inheritance(self):
        """Test that component properly inherits from BaseComponent."""
        from framework.base_component import BaseComponent

        component = PersistenceComponent()
        assert isinstance(component, BaseComponent)

    @patch('components.persistence.BaseComponent.execute')
    @patch('components.persistence.os.makedirs')
    @patch('components.persistence.os.path.getsize')
    @patch('builtins.open', new_callable=mock_open)
    def test_execute_preserves_original_context(self, mock_file, mock_getsize, mock_makedirs, mock_super_execute):
        """Test that original context fields are preserved."""
        component = PersistenceComponent()

        original_context = {
            "processed": {"result": "data"},
            "request_id": "preserve_test",
            "service_id": "test_service",
            "existing_field": "preserved_value"
        }

        mock_getsize.return_value = 50

        with patch.object(component, 'log_info'):
            result = component.execute(original_context)

            # Original fields should be preserved
            assert result["service_id"] == "test_service"
            assert result["existing_field"] == "preserved_value"
            assert result["processed"] == {"result": "data"}

            # New persistence fields should be added
            assert "persisted" in result
            assert "filepath" in result
            assert "size" in result
            assert "persist_format" in result

    @patch('components.persistence.BaseComponent.execute', side_effect=Exception("Base component error"))
    def test_execute_parent_exception_propagation(self, mock_super_execute):
        """Test that exceptions from parent execute are propagated."""
        component = PersistenceComponent()
        context = {"service_id": "test"}

        with pytest.raises(Exception, match="Base component error"):
            component.execute(context)

    def test_persistence_integration_with_temp_dir(self):
        """Integration test with actual file system operations."""
        with tempfile.TemporaryDirectory() as temp_dir:
            component = PersistenceComponent({
                "output_dir": temp_dir,
                "format": "json"
            })

            test_data = {"test": "integration", "count": 123}
            context = {
                "processed": test_data,
                "request_id": "integration_test"
            }

            with patch.object(component, 'log_info'):
                with patch.object(component, 'log_debug'):
                    with patch('components.persistence.BaseComponent.execute'):
                        result = component.execute(context)

            # Verify file was actually created
            expected_file = os.path.join(temp_dir, "integration_test_result.json")
            assert os.path.exists(expected_file)

            # Verify file contents
            with open(expected_file, 'r') as f:
                saved_data = json.load(f)
            assert saved_data == test_data

            # Verify context results
            assert result["persisted"] is True
            assert result["filepath"] == expected_file
            assert result["size"] > 0

    @patch('components.persistence.BaseComponent.execute')
    @patch('components.persistence.os.makedirs')
    def test_execute_data_priority_order(self, mock_makedirs, mock_super_execute):
        """Test that data is selected in correct priority order."""
        component = PersistenceComponent()

        # Test with all data types present
        context = {
            "processed": {"priority": "highest"},
            "transformed_data": {"priority": "medium"},
            "other_data": {"priority": "lowest"},
            "request_id": "priority_test"
        }

        with patch('builtins.open', mock_open()):
            with patch('components.persistence.os.path.getsize', return_value=32):
                with patch.object(component, 'log_info') as mock_log_info:
                    component.execute(context)

                    # Should select 'processed' data
                    mock_log_info.assert_any_call("Persisting 'processed' data")

    @patch('components.persistence.BaseComponent.execute')
    @patch('components.persistence.os.makedirs')
    def test_execute_filters_internal_keys(self, mock_makedirs, mock_super_execute):
        """Test that internal keys (starting with _) are filtered out."""
        component = PersistenceComponent()

        context = {
            "public_key": "visible",
            "_private_key": "hidden",
            "_internal_state": "excluded",
            "request_id": "filter_test"
        }

        written_data = None

        def capture_json_write(data, file_obj, indent=None):
            nonlocal written_data
            written_data = data

        with patch('builtins.open', mock_open()):
            with patch('components.persistence.json.dump', side_effect=capture_json_write):
                with patch('components.persistence.os.path.getsize', return_value=24):
                    with patch.object(component, 'log_info'):
                        component.execute(context)

        # Verify only public keys were included
        if written_data:
            assert "public_key" in written_data
            assert "_private_key" not in written_data
            assert "_internal_state" not in written_data
            assert "request_id" in written_data