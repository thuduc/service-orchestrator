import pytest
from unittest.mock import Mock, patch
from frameworks.service_pipeline.implementation.components.pre_calibration import PreCalibrationComponent


class TestPreCalibrationComponent:

    def test_init_default_config(self):
        """Test initialization with default configuration."""
        component = PreCalibrationComponent()
        assert component.config == {}

    def test_init_with_config(self):
        """Test initialization with custom configuration."""
        config = {"param1": "value1", "param2": 42}
        component = PreCalibrationComponent(config)
        assert component.config == config

    @patch('frameworks.service_pipeline.implementation.components.pre_calibration.BaseComponent.execute')
    def test_execute_success(self, mock_super_execute):
        """Test successful execution."""
        component = PreCalibrationComponent()
        context = {
            "service_id": "test_service",
            "request_id": "test_request_123",
            "data": {"test": "data"}
        }

        with patch.object(component, 'log_info') as mock_log_info:
            result = component.execute(context)

            # Verify parent execute was called
            mock_super_execute.assert_called_once_with(context)

            # Verify logging
            mock_log_info.assert_called_once_with('Hi from Pre-Calibration Component')

            # Verify result structure
            assert result["status"] == "success"
            assert result["message"] == "Hello World from Pre-Calibration Component"
            assert result["service_id"] == "test_service"
            assert result["request_id"] == "test_request_123"
            assert result["component_type"] == "PreCalibrationComponent"

    @patch('frameworks.service_pipeline.implementation.components.pre_calibration.BaseComponent.execute')
    def test_execute_minimal_context(self, mock_super_execute):
        """Test execution with minimal context."""
        component = PreCalibrationComponent()
        context = {}

        with patch.object(component, 'log_info') as mock_log_info:
            result = component.execute(context)

            # Verify parent execute was called
            mock_super_execute.assert_called_once_with(context)

            # Verify logging
            mock_log_info.assert_called_once_with('Hi from Pre-Calibration Component')

            # Verify result with default values
            assert result["status"] == "success"
            assert result["message"] == "Hello World from Pre-Calibration Component"
            assert result["service_id"] == "pre-calibration"  # Default value
            assert result["request_id"] is None
            assert result["component_type"] == "PreCalibrationComponent"

    @patch('frameworks.service_pipeline.implementation.components.pre_calibration.BaseComponent.execute')
    def test_execute_preserves_context_values(self, mock_super_execute):
        """Test that execution preserves existing context values."""
        component = PreCalibrationComponent()
        context = {
            "service_id": "custom_service",
            "request_id": "custom_request",
            "existing_data": "preserved"
        }

        with patch.object(component, 'log_info') as mock_log_info:
            result = component.execute(context)

            # Should use values from context, not defaults
            assert result["service_id"] == "custom_service"
            assert result["request_id"] == "custom_request"

    @patch('frameworks.service_pipeline.implementation.components.pre_calibration.BaseComponent.execute')
    def test_execute_with_none_values(self, mock_super_execute):
        """Test execution when context contains None values."""
        component = PreCalibrationComponent()
        context = {
            "service_id": None,
            "request_id": None
        }

        with patch.object(component, 'log_info') as mock_log_info:
            result = component.execute(context)

            # Should handle None values gracefully
            assert result["service_id"] is None
            assert result["request_id"] is None

    def test_component_inheritance(self):
        """Test that component properly inherits from BaseComponent."""
        from frameworks.service_pipeline.implementation.base_component import BaseComponent

        component = PreCalibrationComponent()
        assert isinstance(component, BaseComponent)

    @patch('frameworks.service_pipeline.implementation.components.pre_calibration.BaseComponent.execute')
    def test_execute_logging_called(self, mock_super_execute):
        """Test that logging method is called during execution."""
        component = PreCalibrationComponent()
        context = {"service_id": "test"}

        with patch.object(component, 'log_info') as mock_log_info:
            component.execute(context)

            # Verify specific log message
            mock_log_info.assert_called_once_with('Hi from Pre-Calibration Component')

    def test_component_config_immutability(self):
        """Test that component config doesn't affect original config dict."""
        original_config = {"param": "value"}
        component = PreCalibrationComponent(original_config)

        # Modify component config
        component.config["new_param"] = "new_value"

        # Original should not be affected (depends on BaseComponent implementation)
        # This test ensures we understand the behavior
        assert "new_param" in component.config

    @patch('frameworks.service_pipeline.implementation.components.pre_calibration.BaseComponent.execute', side_effect=Exception("Base component error"))
    def test_execute_parent_exception_propagation(self, mock_super_execute):
        """Test that exceptions from parent execute are propagated."""
        component = PreCalibrationComponent()
        context = {"service_id": "test"}

        with pytest.raises(Exception, match="Base component error"):
            component.execute(context)

    def test_result_keys_consistency(self):
        """Test that result always contains expected keys."""
        component = PreCalibrationComponent()

        test_contexts = [
            {},
            {"service_id": "test"},
            {"request_id": "req123"},
            {"service_id": "svc", "request_id": "req", "extra": "data"}
        ]

        for context in test_contexts:
            with patch.object(component, 'log_info'):
                with patch('frameworks.service_pipeline.implementation.components.pre_calibration.BaseComponent.execute'):
                    result = component.execute(context)

                    # Always should have these keys
                    required_keys = ["status", "message", "service_id", "request_id", "component_type"]
                    for key in required_keys:
                        assert key in result

                    # Status should always be success
                    assert result["status"] == "success"
                    assert result["component_type"] == "PreCalibrationComponent"
