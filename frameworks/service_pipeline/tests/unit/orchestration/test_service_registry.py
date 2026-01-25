import pytest
import json
import tempfile
import os
from unittest.mock import Mock, patch, mock_open
from frameworks.service_pipeline.orchestration.service_registry import ServiceRegistry


class TestServiceRegistry:

    def test_init_without_config(self):
        """Test initializing registry without configuration file."""
        registry = ServiceRegistry()
        assert registry.config_path is None
        assert registry._registry == {}
        assert registry._executor_cache == {}

    def test_init_with_valid_config(self, tmp_path):
        """Test initializing registry with valid configuration file."""
        config_data = {
            "services": {
                "test_service": {
                    "steps": [
                        {"module": "test.module", "class": "TestComponent"}
                    ]
                }
            }
        }
        config_file = tmp_path / "test_services.json"
        config_file.write_text(json.dumps(config_data))

        registry = ServiceRegistry(str(config_file))
        assert "test_service" in registry._registry
        assert len(registry._registry["test_service"]) == 1

    def test_init_with_nonexistent_config(self):
        """Test initializing with non-existent configuration file."""
        with pytest.raises(FileNotFoundError):
            ServiceRegistry("/nonexistent/path.json")

    def test_init_with_invalid_json(self, tmp_path):
        """Test initializing with invalid JSON configuration."""
        config_file = tmp_path / "invalid.json"
        config_file.write_text("invalid json content")

        with pytest.raises(ValueError, match="Invalid JSON"):
            ServiceRegistry(str(config_file))

    def test_init_with_missing_services_key(self, tmp_path):
        """Test initializing with config missing 'services' key."""
        config_data = {"invalid": "structure"}
        config_file = tmp_path / "invalid_structure.json"
        config_file.write_text(json.dumps(config_data))

        with pytest.raises(ValueError, match="Configuration must contain 'services' key"):
            ServiceRegistry(str(config_file))

    def test_init_with_missing_steps(self, tmp_path):
        """Test initializing with service missing 'steps' array."""
        config_data = {
            "services": {
                "invalid_service": {"config": "no steps"}
            }
        }
        config_file = tmp_path / "no_steps.json"
        config_file.write_text(json.dumps(config_data))

        with pytest.raises(ValueError, match="must have 'steps' array"):
            ServiceRegistry(str(config_file))

    def test_init_with_non_array_steps(self, tmp_path):
        """Test initializing with steps that is not an array."""
        config_data = {
            "services": {
                "invalid_service": {"steps": "not an array"}
            }
        }
        config_file = tmp_path / "invalid_steps.json"
        config_file.write_text(json.dumps(config_data))

        with pytest.raises(ValueError, match="steps must be an array"):
            ServiceRegistry(str(config_file))

    def test_init_with_empty_steps(self, tmp_path):
        """Test initializing with empty steps array."""
        config_data = {
            "services": {
                "empty_service": {"steps": []}
            }
        }
        config_file = tmp_path / "empty_steps.json"
        config_file.write_text(json.dumps(config_data))

        with pytest.raises(ValueError, match="must have at least one step"):
            ServiceRegistry(str(config_file))

    def test_register_service_valid(self):
        """Test registering a service with valid steps."""
        registry = ServiceRegistry()
        steps = [
            {"module": "test.module", "class": "TestComponent", "name": "step1"},
            {"module": "test.module2", "class": "TestComponent2", "name": "step2"}
        ]

        registry.register_service("test_service", steps)
        assert "test_service" in registry._registry
        assert len(registry._registry["test_service"]) == 2

    def test_register_service_auto_name_generation(self):
        """Test that step names are auto-generated when missing."""
        registry = ServiceRegistry()
        steps = [
            {"module": "test.module", "class": "TestComponent"},
            {"module": "test.module2", "class": "TestComponent2"}
        ]

        registry.register_service("test_service", steps)
        registered_steps = registry._registry["test_service"]
        assert registered_steps[0]["name"] == "step_1"
        assert registered_steps[1]["name"] == "step_2"

    def test_register_service_missing_module(self):
        """Test registering service with step missing module."""
        registry = ServiceRegistry()
        steps = [{"class": "TestComponent"}]

        with pytest.raises(ValueError, match="must have 'module' and 'class' fields"):
            registry.register_service("invalid_service", steps)

    def test_register_service_missing_class(self):
        """Test registering service with step missing class."""
        registry = ServiceRegistry()
        steps = [{"module": "test.module"}]

        with pytest.raises(ValueError, match="must have 'module' and 'class' fields"):
            registry.register_service("invalid_service", steps)

    @patch('frameworks.service_pipeline.orchestration.service_registry.StepsExecutor')
    def test_get_executor_new_service(self, mock_steps_executor):
        """Test getting executor for a new service."""
        registry = ServiceRegistry()
        steps = [{"module": "test.module", "class": "TestComponent"}]
        registry.register_service("test_service", steps)

        mock_executor = Mock()
        mock_steps_executor.return_value = mock_executor

        executor = registry.get_executor("test_service")

        assert executor == mock_executor
        assert "test_service" in registry._executor_cache
        mock_steps_executor.assert_called_once_with(steps)

    @patch('frameworks.service_pipeline.orchestration.service_registry.StepsExecutor')
    def test_get_executor_cached_service(self, mock_steps_executor):
        """Test getting executor for service that's already cached."""
        registry = ServiceRegistry()
        steps = [{"module": "test.module", "class": "TestComponent"}]
        registry.register_service("test_service", steps)

        mock_executor = Mock()
        registry._executor_cache["test_service"] = mock_executor

        executor = registry.get_executor("test_service")

        assert executor == mock_executor
        mock_steps_executor.assert_not_called()

    def test_get_executor_unregistered_service(self):
        """Test getting executor for unregistered service."""
        registry = ServiceRegistry()

        with pytest.raises(KeyError, match="Service 'nonexistent' not found"):
            registry.get_executor("nonexistent")

    def test_list_services_empty(self):
        """Test listing services when registry is empty."""
        registry = ServiceRegistry()
        services = registry.list_services()
        assert services == {}

    def test_list_services_with_data(self):
        """Test listing services with registered services."""
        registry = ServiceRegistry()
        registry.register_service("service1", [{"module": "m1", "class": "c1"}])
        registry.register_service("service2", [
            {"module": "m2", "class": "c2"},
            {"module": "m3", "class": "c3"}
        ])

        services = registry.list_services()
        assert services == {"service1": 1, "service2": 2}

    def test_get_service_info_valid(self):
        """Test getting service info for valid service."""
        registry = ServiceRegistry()
        steps = [
            {
                "name": "step1",
                "module": "test.module",
                "class": "TestComponent",
                "input_mapping": {"key": "value"},
                "output_mapping": {"result": "output"}
            },
            {
                "name": "step2",
                "module": "test.module2",
                "class": "TestComponent2"
            }
        ]
        registry.register_service("test_service", steps)

        info = registry.get_service_info("test_service")

        assert info["service_id"] == "test_service"
        assert len(info["steps"]) == 2
        assert info["steps"][0]["name"] == "step1"
        assert info["steps"][0]["module"] == "test.module"
        assert info["steps"][0]["class"] == "TestComponent"
        assert info["steps"][0]["has_input_mapping"] is True
        assert info["steps"][0]["has_output_mapping"] is True
        assert info["steps"][1]["has_input_mapping"] is False
        assert info["steps"][1]["has_output_mapping"] is False

    def test_get_service_info_unregistered(self):
        """Test getting service info for unregistered service."""
        registry = ServiceRegistry()

        with pytest.raises(KeyError, match="Service 'nonexistent' not found"):
            registry.get_service_info("nonexistent")


class TestServiceRegistryIntegration:
    """Integration tests for ServiceRegistry with real file I/O."""

    def test_full_lifecycle_with_file(self, tmp_path):
        """Test complete lifecycle with actual file configuration."""
        config_data = {
            "services": {
                "multi_step_service": {
                    "steps": [
                        {
                            "name": "validation",
                            "module": "frameworks.service_pipeline.implementation.components.validation",
                            "class": "ValidationComponent",
                            "config": {"strict": True}
                        },
                        {
                            "name": "transformation",
                            "module": "frameworks.service_pipeline.implementation.components.transformation",
                            "class": "TransformationComponent",
                            "config": {"format": "json"}
                        }
                    ]
                },
                "single_step_service": {
                    "steps": [
                        {
                            "module": "frameworks.service_pipeline.implementation.components.pre_calibration",
                            "class": "PreCalibrationComponent"
                        }
                    ]
                }
            }
        }

        config_file = tmp_path / "integration_test.json"
        config_file.write_text(json.dumps(config_data))

        # Initialize registry with config file
        registry = ServiceRegistry(str(config_file))

        # Verify services are loaded
        services = registry.list_services()
        assert services == {"multi_step_service": 2, "single_step_service": 1}

        # Verify service info
        info = registry.get_service_info("multi_step_service")
        assert len(info["steps"]) == 2
        assert info["steps"][0]["name"] == "validation"

        # Verify auto-generated name
        single_info = registry.get_service_info("single_step_service")
        assert single_info["steps"][0]["name"] == "step_1"
