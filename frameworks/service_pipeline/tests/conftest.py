import pytest
import json
import os
from unittest.mock import Mock, patch
from typing import Dict, Any


@pytest.fixture
def sample_context():
    """Basic context fixture for testing."""
    return {
        "service_id": "test_service",
        "request_id": "test_request_123",
        "user_id": "test_user",
        "data": {"test": "value"}
    }


@pytest.fixture
def sample_service_config():
    """Sample service configuration for testing."""
    return {
        "module": "frameworks.service_pipeline.implementation.components.pre_calibration",
        "class": "PreCalibrationComponent",
        "config": {
            "param1": "value1",
            "param2": 42
        }
    }


@pytest.fixture
def test_services_config():
    """Complete services configuration for testing."""
    return {
        "services": {
            "test_service": {
                "module": "frameworks.service_pipeline.implementation.components.pre_calibration",
                "class": "PreCalibrationComponent",
                "config": {"test": True}
            },
            "simulation_service": {
                "module": "frameworks.service_pipeline.implementation.components.simulation",
                "class": "SimulationComponent",
                "config": {"iterations": 100}
            }
        }
    }


@pytest.fixture
def test_interceptors_config():
    """Sample interceptor configuration for testing."""
    return {
        "interceptors": {
            "logging": {
                "module": "frameworks.service_pipeline.implementation.interceptors.logging",
                "class": "LoggingInterceptor",
                "enabled": True,
                "order": 10,
                "config": {"log_level": "DEBUG"}
            },
            "validation": {
                "module": "frameworks.service_pipeline.implementation.interceptors.validation",
                "class": "ValidationInterceptor",
                "enabled": True,
                "order": 20,
                "config": {"strict_mode": True}
            }
        }
    }


@pytest.fixture
def mock_component():
    """Mock component for testing."""
    component = Mock()
    component.execute.return_value = {"status": "success", "result": "mock_result"}
    return component


@pytest.fixture
def mock_interceptor():
    """Mock interceptor for testing."""
    interceptor = Mock()
    interceptor.before.return_value = {"processed": True}
    interceptor.after.return_value = {"processed": True}
    interceptor.on_error.return_value = None
    return interceptor


@pytest.fixture
def temp_config_file(tmp_path):
    """Creates a temporary configuration file."""
    def _create_config(config_data: Dict[str, Any], filename: str = "test_config.json"):
        config_file = tmp_path / filename
        with open(config_file, 'w') as f:
            json.dump(config_data, f)
        return str(config_file)
    return _create_config


@pytest.fixture
def mock_logger():
    """Mock logger for testing."""
    with patch('logging.getLogger') as mock_get_logger:
        logger = Mock()
        mock_get_logger.return_value = logger
        yield logger


@pytest.fixture(autouse=True)
def reset_registries():
    """Reset service and interceptor registries before each test."""
    # This will ensure clean state between tests
    with patch.dict('sys.modules', {}, clear=False):
        yield


@pytest.fixture
def invalid_config_data():
    """Invalid configuration data for testing error scenarios."""
    return [
        # Missing required fields
        {"services": {"invalid": {"module": "missing_class"}}},
        # Invalid JSON structure
        {"invalid_structure": "not_a_service_config"},
        # Non-existent module
        {"services": {"test": {"module": "non.existent.module", "class": "TestClass"}}},
        # Empty configuration
        {},
    ]


@pytest.fixture
def performance_test_data():
    """Large dataset for performance testing."""
    return {
        "large_context": {
            "service_id": "performance_test",
            "data": {f"key_{i}": f"value_{i}" for i in range(1000)}
        },
        "concurrent_contexts": [
            {
                "service_id": f"service_{i}",
                "request_id": f"req_{i}",
                "data": {"index": i}
            }
            for i in range(100)
        ]
    }
