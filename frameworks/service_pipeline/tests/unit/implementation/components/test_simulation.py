import pytest
from unittest.mock import Mock, patch
from frameworks.service_pipeline.implementation.components.simulation import SimulationComponent


class TestSimulationComponent:

    def test_init_default_config(self):
        """Test initialization with default configuration."""
        component = SimulationComponent()
        assert component.config == {}

    def test_init_with_config(self):
        """Test initialization with custom configuration."""
        config = {"iterations": 100, "mode": "fast"}
        component = SimulationComponent(config)
        assert component.config == config

    @patch('frameworks.service_pipeline.implementation.components.simulation.BaseComponent.execute')
    def test_execute_success(self, mock_super_execute):
        """Test successful execution."""
        component = SimulationComponent()
        context = {
            "service_id": "simulation_service",
            "request_id": "sim_request_456",
            "data": {"simulation_params": {"duration": 60}}
        }

        with patch.object(component, 'log_info') as mock_log_info:
            result = component.execute(context)

            # Verify parent execute was called
            mock_super_execute.assert_called_once_with(context)

            # Verify logging
            mock_log_info.assert_called_once_with('Hi from Simulation Component')

            # Verify result structure
            assert result["status"] == "success"
            assert result["message"] == "Hello World from Simulation Component"
            assert result["service_id"] == "simulation_service"
            assert result["request_id"] == "sim_request_456"
            assert result["component_type"] == "SimulationComponent"

    @patch('frameworks.service_pipeline.implementation.components.simulation.BaseComponent.execute')
    def test_execute_minimal_context(self, mock_super_execute):
        """Test execution with minimal context."""
        component = SimulationComponent()
        context = {}

        with patch.object(component, 'log_info') as mock_log_info:
            result = component.execute(context)

            # Verify parent execute was called
            mock_super_execute.assert_called_once_with(context)

            # Verify logging
            mock_log_info.assert_called_once_with('Hi from Simulation Component')

            # Verify result with default values
            assert result["status"] == "success"
            assert result["message"] == "Hello World from Simulation Component"
            assert result["service_id"] == "simulation"  # Default value
            assert result["request_id"] is None
            assert result["component_type"] == "SimulationComponent"

    @patch('frameworks.service_pipeline.implementation.components.simulation.BaseComponent.execute')
    def test_execute_preserves_context_values(self, mock_super_execute):
        """Test that execution preserves existing context values."""
        component = SimulationComponent()
        context = {
            "service_id": "custom_simulation",
            "request_id": "custom_sim_request",
            "model_params": {"complexity": "high"}
        }

        with patch.object(component, 'log_info') as mock_log_info:
            result = component.execute(context)

            # Should use values from context, not defaults
            assert result["service_id"] == "custom_simulation"
            assert result["request_id"] == "custom_sim_request"

    @patch('frameworks.service_pipeline.implementation.components.simulation.BaseComponent.execute')
    def test_execute_with_none_values(self, mock_super_execute):
        """Test execution when context contains None values."""
        component = SimulationComponent()
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

        component = SimulationComponent()
        assert isinstance(component, BaseComponent)

    @patch('frameworks.service_pipeline.implementation.components.simulation.BaseComponent.execute')
    def test_execute_logging_called(self, mock_super_execute):
        """Test that logging method is called during execution."""
        component = SimulationComponent()
        context = {"service_id": "sim_test"}

        with patch.object(component, 'log_info') as mock_log_info:
            component.execute(context)

            # Verify specific log message
            mock_log_info.assert_called_once_with('Hi from Simulation Component')

    def test_component_config_access(self):
        """Test that component can access configuration parameters."""
        config = {"iterations": 1000, "seed": 42, "output_format": "json"}
        component = SimulationComponent(config)

        # Should be able to access config parameters
        assert component.config["iterations"] == 1000
        assert component.config["seed"] == 42
        assert component.config["output_format"] == "json"

    @patch('frameworks.service_pipeline.implementation.components.simulation.BaseComponent.execute', side_effect=Exception("Base component error"))
    def test_execute_parent_exception_propagation(self, mock_super_execute):
        """Test that exceptions from parent execute are propagated."""
        component = SimulationComponent()
        context = {"service_id": "test_sim"}

        with pytest.raises(Exception, match="Base component error"):
            component.execute(context)

    def test_result_keys_consistency(self):
        """Test that result always contains expected keys."""
        component = SimulationComponent()

        test_contexts = [
            {},
            {"service_id": "test_simulation"},
            {"request_id": "sim_req123"},
            {"service_id": "sim_svc", "request_id": "sim_req", "parameters": {"param": "value"}}
        ]

        for context in test_contexts:
            with patch.object(component, 'log_info'):
                with patch('frameworks.service_pipeline.implementation.components.simulation.BaseComponent.execute'):
                    result = component.execute(context)

                    # Always should have these keys
                    required_keys = ["status", "message", "service_id", "request_id", "component_type"]
                    for key in required_keys:
                        assert key in result

                    # Status should always be success
                    assert result["status"] == "success"
                    assert result["component_type"] == "SimulationComponent"

    def test_component_unique_identifier(self):
        """Test that component has unique type identifier."""
        component = SimulationComponent()

        with patch.object(component, 'log_info'):
            with patch('frameworks.service_pipeline.implementation.components.simulation.BaseComponent.execute'):
                result = component.execute({})

                # Should have unique component type
                assert result["component_type"] == "SimulationComponent"

    @patch('frameworks.service_pipeline.implementation.components.simulation.BaseComponent.execute')
    def test_execute_with_complex_context(self, mock_super_execute):
        """Test execution with complex context data."""
        component = SimulationComponent()
        context = {
            "service_id": "complex_simulation",
            "request_id": "complex_req_789",
            "data": {
                "model": {
                    "type": "monte_carlo",
                    "iterations": 10000,
                    "parameters": {
                        "volatility": 0.2,
                        "drift": 0.05
                    }
                },
                "outputs": ["price_path", "statistics"]
            },
            "metadata": {
                "user_id": "user123",
                "timestamp": "2023-01-01T00:00:00Z"
            }
        }

        with patch.object(component, 'log_info') as mock_log_info:
            result = component.execute(context)

            # Should handle complex context gracefully
            assert result["status"] == "success"
            assert result["service_id"] == "complex_simulation"
            assert result["request_id"] == "complex_req_789"
            assert result["component_type"] == "SimulationComponent"
