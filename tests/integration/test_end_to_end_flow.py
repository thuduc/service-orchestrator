"""
End-to-End Integration Tests

Tests the complete flow from service entrypoint through interceptor pipeline
to component execution using real configurations and components.
"""
import pytest
import json
import tempfile
import os
from framework.service_registry import ServiceRegistry
from framework.service_entrypoint import ServiceEntrypoint
from framework.interceptor_pipeline import InterceptorPipeline
from framework.interceptor_registry import InterceptorRegistry


class TestEndToEndFlow:
    """Integration tests for complete service execution flows."""

    @pytest.fixture
    def temp_configs(self):
        """Create temporary configuration files for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create services configuration
            services_config = {
                "services": {
                    "integration_test_service": {
                        "steps": [
                            {
                                "name": "validation_step",
                                "module": "components.validation",
                                "class": "ValidationComponent",
                                "config": {"required_fields": ["data"]}
                            },
                            {
                                "name": "transformation_step",
                                "module": "components.transformation",
                                "class": "TransformationComponent",
                                "config": {"transform_type": "uppercase"}
                            },
                            {
                                "name": "persistence_step",
                                "module": "components.persistence",
                                "class": "PersistenceComponent",
                                "config": {
                                    "output_dir": os.path.join(temp_dir, "output"),
                                    "format": "json"
                                }
                            }
                        ]
                    }
                }
            }

            services_file = os.path.join(temp_dir, "services.json")
            with open(services_file, 'w') as f:
                json.dump(services_config, f)

            # Create interceptors configuration
            interceptors_config = {
                "interceptors": {
                    "logging": {
                        "module": "interceptors.logging",
                        "class": "LoggingInterceptor",
                        "enabled": True,
                        "order": 10,
                        "config": {
                            "log_level": "DEBUG",
                            "destinations": ["file"],
                            "file_path": os.path.join(temp_dir, "integration.log")
                        }
                    },
                    "validation": {
                        "module": "interceptors.validation",
                        "class": "ValidationInterceptor",
                        "enabled": True,
                        "order": 20,
                        "config": {
                            "validate_request": True,
                            "validate_response": True,
                            "strict_mode": False
                        }
                    }
                }
            }

            interceptors_file = os.path.join(temp_dir, "interceptors.json")
            with open(interceptors_file, 'w') as f:
                json.dump(interceptors_config, f)

            yield {
                "temp_dir": temp_dir,
                "services_file": services_file,
                "interceptors_file": interceptors_file,
                "output_dir": os.path.join(temp_dir, "output"),
                "log_file": os.path.join(temp_dir, "integration.log")
            }

    def test_complete_service_execution_success(self, temp_configs):
        """Test successful end-to-end service execution."""
        # Initialize service registry
        service_registry = ServiceRegistry(temp_configs["services_file"])

        # Initialize service entrypoint with interceptors
        entrypoint = ServiceEntrypoint(
            service_registry,
            interceptor_config_path=temp_configs["interceptors_file"]
        )

        # Execute service
        context = {
            "service_id": "integration_test_service",
            "request_id": "e2e_test_001",
            "data": {"message": "hello world", "count": 42}
        }

        result = entrypoint.execute(context)

        # Verify execution results
        assert result["validation_passed"] is True
        assert "transformed_data" in result
        assert result["transformed_data"]["message"] == "HELLO WORLD"
        assert result["transformed_data"]["count"] == 42
        assert result["persisted"] is True
        assert os.path.exists(result["filepath"])

        # Verify persisted data
        with open(result["filepath"], 'r') as f:
            saved_data = json.load(f)
        assert saved_data["message"] == "HELLO WORLD"
        assert saved_data["count"] == 42

        # Verify logging
        assert os.path.exists(temp_configs["log_file"])

    def test_service_execution_with_validation_failure(self, temp_configs):
        """Test service execution when validation fails."""
        service_registry = ServiceRegistry(temp_configs["services_file"])
        entrypoint = ServiceEntrypoint(
            service_registry,
            interceptor_config_path=temp_configs["interceptors_file"]
        )

        # Context missing required 'data' field
        context = {
            "service_id": "integration_test_service",
            "request_id": "e2e_validation_fail_001"
            # Missing 'data' field required by validation component
        }

        result = entrypoint.execute(context)

        # Validation should fail but execution should continue (non-strict mode)
        assert result["validation_passed"] is False
        assert "validation_errors" in result
        assert len(result["validation_errors"]) > 0

    def test_service_execution_with_interceptor_chain(self, temp_configs):
        """Test that interceptor chain executes in correct order."""
        service_registry = ServiceRegistry(temp_configs["services_file"])
        entrypoint = ServiceEntrypoint(
            service_registry,
            interceptor_config_path=temp_configs["interceptors_file"]
        )

        context = {
            "service_id": "integration_test_service",
            "request_id": "interceptor_chain_001",
            "data": {"test": "interceptor_order"}
        }

        result = entrypoint.execute(context)

        # Both interceptors and components should have executed
        assert result["validation_passed"] is True
        assert "transformed_data" in result
        assert result["persisted"] is True

        # Check log file contains interceptor logs
        with open(temp_configs["log_file"], 'r') as f:
            log_content = f.read()
        assert "interceptor_chain_001" in log_content
        assert "Starting execution" in log_content
        assert "Completed execution" in log_content


class TestServiceConfiguration:
    """Integration tests for service configuration loading and validation."""

    def test_invalid_service_configuration(self):
        """Test handling of invalid service configuration."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            # Invalid configuration - missing required fields
            json.dump({"invalid": "config"}, f)
            f.flush()

            with pytest.raises(ValueError, match="Configuration must contain 'services' key"):
                ServiceRegistry(f.name)

            os.unlink(f.name)

    def test_nonexistent_component_module(self):
        """Test handling of nonexistent component modules."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            config = {
                "services": {
                    "test_service": {
                        "steps": [
                            {
                                "module": "nonexistent.module",
                                "class": "NonexistentComponent"
                            }
                        ]
                    }
                }
            }
            json.dump(config, f)
            f.flush()

            registry = ServiceRegistry(f.name)

            # Should fail when trying to get executor
            with pytest.raises(RuntimeError, match="Failed to load step"):
                registry.get_executor("test_service")

            os.unlink(f.name)


class TestInterceptorIntegration:
    """Integration tests for interceptor pipeline integration."""

    def test_interceptor_error_handling(self):
        """Test interceptor pipeline error handling."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create interceptor config with strict validation
            interceptors_config = {
                "interceptors": {
                    "strict_validation": {
                        "module": "interceptors.validation",
                        "class": "ValidationInterceptor",
                        "enabled": True,
                        "order": 10,
                        "config": {
                            "validate_request": True,
                            "strict_mode": True,
                            "required_fields": ["service_id", "required_field"]
                        }
                    }
                }
            }

            interceptors_file = os.path.join(temp_dir, "strict_interceptors.json")
            with open(interceptors_file, 'w') as f:
                json.dump(interceptors_config, f)

            # Create simple service config
            services_config = {
                "services": {
                    "simple_service": {
                        "steps": [
                            {
                                "module": "components.pre_calibration",
                                "class": "PreCalibrationComponent"
                            }
                        ]
                    }
                }
            }

            services_file = os.path.join(temp_dir, "simple_services.json")
            with open(services_file, 'w') as f:
                json.dump(services_config, f)

            # Initialize and test
            service_registry = ServiceRegistry(services_file)
            entrypoint = ServiceEntrypoint(
                service_registry,
                interceptor_config_path=interceptors_file
            )

            # This should fail validation in strict mode
            context = {"service_id": "simple_service"}  # Missing required_field

            with pytest.raises(Exception):  # ValidationError should be raised
                entrypoint.execute(context)


class TestPerformanceIntegration:
    """Basic performance integration tests."""

    def test_service_execution_performance(self):
        """Test basic service execution performance."""
        import time

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create minimal service config
            services_config = {
                "services": {
                    "perf_test_service": {
                        "steps": [
                            {
                                "module": "components.pre_calibration",
                                "class": "PreCalibrationComponent"
                            }
                        ]
                    }
                }
            }

            services_file = os.path.join(temp_dir, "perf_services.json")
            with open(services_file, 'w') as f:
                json.dump(services_config, f)

            service_registry = ServiceRegistry(services_file)
            entrypoint = ServiceEntrypoint(service_registry)

            context = {
                "service_id": "perf_test_service",
                "request_id": "perf_test_001"
            }

            # Measure execution time
            start_time = time.time()
            result = entrypoint.execute(context)
            execution_time = time.time() - start_time

            # Basic performance check - should execute quickly
            assert execution_time < 1.0  # Should complete within 1 second
            assert result["status"] == "success"

    def test_concurrent_execution_safety(self):
        """Test that concurrent executions don't interfere with each other."""
        import threading
        import time

        with tempfile.TemporaryDirectory() as temp_dir:
            services_config = {
                "services": {
                    "concurrent_test_service": {
                        "steps": [
                            {
                                "module": "components.validation",
                                "class": "ValidationComponent",
                                "config": {"required_fields": ["thread_id"]}
                            }
                        ]
                    }
                }
            }

            services_file = os.path.join(temp_dir, "concurrent_services.json")
            with open(services_file, 'w') as f:
                json.dump(services_config, f)

            service_registry = ServiceRegistry(services_file)
            entrypoint = ServiceEntrypoint(service_registry)

            results = {}
            errors = {}

            def execute_service(thread_id):
                try:
                    context = {
                        "service_id": "concurrent_test_service",
                        "request_id": f"concurrent_req_{thread_id}",
                        "thread_id": thread_id,
                        "data": {"thread_data": f"data_from_thread_{thread_id}"}
                    }
                    result = entrypoint.execute(context)
                    results[thread_id] = result
                except Exception as e:
                    errors[thread_id] = str(e)

            # Execute multiple threads concurrently
            threads = []
            num_threads = 5

            for i in range(num_threads):
                thread = threading.Thread(target=execute_service, args=(i,))
                threads.append(thread)
                thread.start()

            # Wait for all threads to complete
            for thread in threads:
                thread.join()

            # Verify all executions succeeded
            assert len(errors) == 0, f"Errors in concurrent execution: {errors}"
            assert len(results) == num_threads

            # Verify each thread got its own result
            for thread_id in range(num_threads):
                assert thread_id in results
                result = results[thread_id]
                assert result["validation_passed"] is True
                assert result["thread_id"] == thread_id
