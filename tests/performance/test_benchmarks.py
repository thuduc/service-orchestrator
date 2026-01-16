"""
Performance Benchmarks

Performance tests using pytest-benchmark to measure execution times
and establish baseline performance metrics.
"""
import pytest
import json
import tempfile
import os
import time
from framework.service_registry import ServiceRegistry
from framework.service_entrypoint import ServiceEntrypoint


@pytest.fixture
def benchmark_service_config():
    """Create service configuration for benchmarking."""
    with tempfile.TemporaryDirectory() as temp_dir:
        services_config = {
            "services": {
                "benchmark_service": {
                    "steps": [
                        {
                            "name": "validation",
                            "module": "components.validation",
                            "class": "ValidationComponent",
                            "config": {"required_fields": ["data"]}
                        },
                        {
                            "name": "transformation",
                            "module": "components.transformation",
                            "class": "TransformationComponent",
                            "config": {"transform_type": "uppercase"}
                        }
                    ]
                },
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

        services_file = os.path.join(temp_dir, "benchmark_services.json")
        with open(services_file, 'w') as f:
            json.dump(services_config, f)

        yield {
            "services_file": services_file,
            "temp_dir": temp_dir
        }


class TestServiceExecutionBenchmarks:
    """Benchmark tests for service execution performance."""

    def test_simple_service_execution_benchmark(self, benchmark, benchmark_service_config):
        """Benchmark simple single-component service execution."""
        service_registry = ServiceRegistry(benchmark_service_config["services_file"])
        entrypoint = ServiceEntrypoint(service_registry)

        context = {
            "service_id": "simple_service",
            "request_id": "benchmark_simple"
        }

        # Benchmark the execution
        result = benchmark(entrypoint.execute, context)

        # Verify correctness
        assert result["status"] == "success"
        assert result["component_type"] == "PreCalibrationComponent"

    def test_multi_step_service_execution_benchmark(self, benchmark, benchmark_service_config):
        """Benchmark multi-step service execution."""
        service_registry = ServiceRegistry(benchmark_service_config["services_file"])
        entrypoint = ServiceEntrypoint(service_registry)

        context = {
            "service_id": "benchmark_service",
            "request_id": "benchmark_multi_step",
            "data": {"message": "performance test", "value": 123}
        }

        # Benchmark the execution
        result = benchmark(entrypoint.execute, context)

        # Verify correctness
        assert result["validation_passed"] is True
        assert result["transformed_data"]["message"] == "PERFORMANCE TEST"
        assert result["transformed_data"]["value"] == 123

    def test_service_registry_lookup_benchmark(self, benchmark, benchmark_service_config):
        """Benchmark service registry executor lookup."""
        service_registry = ServiceRegistry(benchmark_service_config["services_file"])

        # Benchmark executor retrieval (should use caching)
        def get_executor():
            return service_registry.get_executor("simple_service")

        executor = benchmark(get_executor)
        assert executor is not None

    def test_large_context_processing_benchmark(self, benchmark, benchmark_service_config):
        """Benchmark processing with large context data."""
        service_registry = ServiceRegistry(benchmark_service_config["services_file"])
        entrypoint = ServiceEntrypoint(service_registry)

        # Create large context data
        large_data = {f"key_{i}": f"value_{i}" * 100 for i in range(100)}

        context = {
            "service_id": "benchmark_service",
            "request_id": "benchmark_large_context",
            "data": large_data
        }

        # Benchmark the execution
        result = benchmark(entrypoint.execute, context)

        # Verify correctness
        assert result["validation_passed"] is True
        assert len(result["transformed_data"]) == 100


class TestMemoryPerformance:
    """Memory usage performance tests."""

    def test_memory_usage_single_execution(self, benchmark_service_config):
        """Test memory usage for single service execution."""
        import tracemalloc

        service_registry = ServiceRegistry(benchmark_service_config["services_file"])
        entrypoint = ServiceEntrypoint(service_registry)

        context = {
            "service_id": "simple_service",
            "request_id": "memory_test"
        }

        # Measure memory usage
        tracemalloc.start()

        result = entrypoint.execute(context)

        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        # Verify execution succeeded
        assert result["status"] == "success"

        # Memory usage should be reasonable (less than 10MB for simple service)
        assert peak < 10 * 1024 * 1024, f"Peak memory usage too high: {peak} bytes"

    def test_memory_usage_repeated_executions(self, benchmark_service_config):
        """Test memory usage for repeated executions to detect leaks."""
        import tracemalloc
        import gc

        service_registry = ServiceRegistry(benchmark_service_config["services_file"])
        entrypoint = ServiceEntrypoint(service_registry)

        # Force garbage collection before test
        gc.collect()

        tracemalloc.start()

        # Execute multiple times
        memory_snapshots = []

        for i in range(10):
            context = {
                "service_id": "simple_service",
                "request_id": f"memory_leak_test_{i}"
            }

            result = entrypoint.execute(context)
            assert result["status"] == "success"

            current, peak = tracemalloc.get_traced_memory()
            memory_snapshots.append(current)

            # Force garbage collection between iterations
            gc.collect()

        tracemalloc.stop()

        # Memory usage should not increase significantly over iterations
        initial_memory = memory_snapshots[0]
        final_memory = memory_snapshots[-1]
        memory_increase = final_memory - initial_memory

        # Allow some increase but not more than 5MB
        assert memory_increase < 5 * 1024 * 1024, \
            f"Potential memory leak detected: {memory_increase} bytes increase"


class TestConcurrencyPerformance:
    """Concurrency performance tests."""

    def test_concurrent_execution_throughput(self, benchmark_service_config):
        """Test throughput under concurrent load."""
        import threading
        import time
        from concurrent.futures import ThreadPoolExecutor, as_completed

        service_registry = ServiceRegistry(benchmark_service_config["services_file"])
        entrypoint = ServiceEntrypoint(service_registry)

        def execute_service(request_id):
            context = {
                "service_id": "simple_service",
                "request_id": f"concurrent_{request_id}"
            }
            start_time = time.time()
            result = entrypoint.execute(context)
            end_time = time.time()
            return result, end_time - start_time

        # Test with multiple concurrent executions
        num_requests = 20
        max_workers = 5

        start_time = time.time()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(execute_service, i) for i in range(num_requests)]
            results = []

            for future in as_completed(futures):
                result, execution_time = future.result()
                results.append((result, execution_time))

        total_time = time.time() - start_time

        # Verify all executions succeeded
        assert len(results) == num_requests
        for result, execution_time in results:
            assert result["status"] == "success"

        # Calculate throughput (requests per second)
        throughput = num_requests / total_time

        # Should achieve reasonable throughput
        assert throughput > 10, f"Throughput too low: {throughput} req/sec"

        # Individual execution times should be reasonable
        execution_times = [exec_time for _, exec_time in results]
        avg_execution_time = sum(execution_times) / len(execution_times)
        max_execution_time = max(execution_times)

        assert avg_execution_time < 0.1, f"Average execution time too high: {avg_execution_time}s"
        assert max_execution_time < 0.5, f"Max execution time too high: {max_execution_time}s"


class TestScalabilityBenchmarks:
    """Scalability benchmark tests."""

    def test_context_size_scalability(self, benchmark, benchmark_service_config):
        """Test how performance scales with context size."""
        service_registry = ServiceRegistry(benchmark_service_config["services_file"])
        entrypoint = ServiceEntrypoint(service_registry)

        # Test with different context sizes
        sizes = [10, 100, 1000]
        execution_times = []

        for size in sizes:
            context = {
                "service_id": "benchmark_service",
                "request_id": f"scalability_test_{size}",
                "data": {f"item_{i}": f"data_{i}" for i in range(size)}
            }

            # Measure execution time
            start_time = time.time()
            result = entrypoint.execute(context)
            execution_time = time.time() - start_time

            execution_times.append(execution_time)

            # Verify correctness
            assert result["validation_passed"] is True

        # Execution time should scale reasonably (not exponentially)
        # Allow for some increase but not more than linear
        time_ratio_10_to_100 = execution_times[1] / execution_times[0]
        time_ratio_100_to_1000 = execution_times[2] / execution_times[1]

        # Should not scale worse than O(n)
        assert time_ratio_10_to_100 < 20, "Performance degrades too much with 10x data increase"
        assert time_ratio_100_to_1000 < 20, "Performance degrades too much with 10x data increase"

    def test_service_count_scalability(self, benchmark):
        """Test how performance scales with number of registered services."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create config with many services
            services_config = {"services": {}}

            for i in range(100):
                services_config["services"][f"service_{i}"] = {
                    "steps": [
                        {
                            "module": "components.pre_calibration",
                            "class": "PreCalibrationComponent"
                        }
                    ]
                }

            services_file = os.path.join(temp_dir, "many_services.json")
            with open(services_file, 'w') as f:
                json.dump(services_config, f)

            service_registry = ServiceRegistry(services_file)
            entrypoint = ServiceEntrypoint(service_registry)

            context = {
                "service_id": "service_50",  # Execute service in the middle
                "request_id": "scalability_many_services"
            }

            # Benchmark execution - should not be affected by number of registered services
            result = benchmark(entrypoint.execute, context)

            assert result["status"] == "success"