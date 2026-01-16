# Service Orchestrator Framework - Testing Infrastructure

This directory contains the complete testing infrastructure for the Service Orchestrator Framework, designed to ensure comprehensive coverage and reliable testing of all framework components.

## Test Structure

```
tests/
├── __init__.py                    # Test package initialization
├── conftest.py                   # Pytest configuration and shared fixtures
├── pytest.ini                   # Pytest configuration file
├── requirements-test.txt         # Testing dependencies
├── unit/                        # Unit tests
│   ├── framework/              # Framework core tests
│   │   ├── test_service_registry.py
│   │   ├── test_steps_executor.py
│   │   ├── test_service_entrypoint.py
│   │   ├── test_middleware_pipeline.py
│   │   └── test_middleware_registry.py
│   ├── components/             # Component tests
│   │   ├── test_pre_calibration.py
│   │   ├── test_simulation.py
│   │   ├── test_validation.py
│   │   ├── test_transformation.py
│   │   └── test_persistence.py
│   └── middlewares/            # Middleware tests
│       ├── test_logging.py
│       └── test_validation.py
├── integration/                # Integration tests
│   └── test_end_to_end_flow.py
├── performance/                # Performance tests
│   └── test_benchmarks.py
└── fixtures/                   # Test data and configurations
    ├── configs/               # Test configuration files
    │   ├── test_services.json
    │   ├── test_middlewares.json
    │   └── invalid_configs/   # Invalid configs for error testing
    └── data/                  # Sample test data
        └── sample_contexts.json
```

## Running Tests

### Prerequisites

Install testing dependencies:

```bash
pip install -r requirements-test.txt
```

### Basic Test Execution

Run all tests:
```bash
pytest
```

Run tests with coverage:
```bash
pytest --cov=framework --cov=components --cov=middlewares --cov-report=html --cov-report=term-missing
```

Run specific test categories:
```bash
# Unit tests only
pytest tests/unit/

# Integration tests only
pytest tests/integration/

# Performance tests only
pytest tests/performance/

# Specific component tests
pytest tests/unit/framework/test_service_registry.py
```

### Test Output Options

Generate HTML coverage report:
```bash
pytest --cov=framework --cov=components --cov=middlewares --cov-report=html
# Open htmlcov/index.html in browser
```

Run tests with verbose output:
```bash
pytest -v
```

Run tests and stop on first failure:
```bash
pytest -x
```

## Test Categories

### Unit Tests (`tests/unit/`)

Comprehensive unit tests for all framework components:

- **Framework Core Tests**: Test service registry, steps executor, middleware pipeline, and service entrypoint
- **Component Tests**: Test individual business logic components
- **Middleware Tests**: Test middleware implementations

**Coverage Target**: >85% for framework, >90% for components/middleware

### Integration Tests (`tests/integration/`)

End-to-end integration tests that verify:

- Complete service execution flows
- Middleware pipeline integration
- Configuration loading and validation
- Error handling across components
- Concurrent execution safety

### Performance Tests (`tests/performance/`)

Performance benchmarks and scalability tests:

- Service execution benchmarks
- Memory usage monitoring
- Concurrency performance
- Scalability with data size
- Throughput testing

Run performance tests with benchmarking:
```bash
pytest tests/performance/ --benchmark-only
```

## Test Fixtures and Data

### Configuration Fixtures (`tests/fixtures/configs/`)

- `test_services.json`: Complete service configurations for testing
- `test_middlewares.json`: Middleware configurations with various settings
- `invalid_configs/`: Invalid configurations for error testing

### Sample Data (`tests/fixtures/data/`)

- `sample_contexts.json`: Various request contexts for different test scenarios

### Shared Fixtures (`tests/conftest.py`)

Common fixtures available to all tests:

- `sample_context`: Basic request context
- `sample_service_config`: Service configuration template
- `test_services_config`: Complete services configuration
- `test_middlewares_config`: Middleware configuration
- `mock_component`: Mock component for testing
- `temp_config_file`: Temporary configuration file creator
- `invalid_config_data`: Invalid configurations for error testing
- `performance_test_data`: Large datasets for performance testing

## Test Patterns and Best Practices

### Unit Test Patterns

```python
class TestComponentName:
    def test_init_default_config(self):
        """Test initialization with default configuration."""
        component = ComponentName()
        assert component.config == {}

    def test_execute_success(self):
        """Test successful execution."""
        # Setup mocks and test data
        # Execute the method
        # Assert results and verify calls

    def test_execute_error_handling(self):
        """Test error handling scenarios."""
        # Test exception handling and error conditions
```

### Integration Test Patterns

```python
def test_end_to_end_flow(self, temp_configs):
    """Test complete service execution flow."""
    # Setup service registry and entrypoint
    # Execute service with real configurations
    # Verify complete flow and side effects
```

### Performance Test Patterns

```python
def test_performance_benchmark(self, benchmark):
    """Benchmark service execution performance."""
    result = benchmark(service.execute, context)
    assert result["status"] == "success"
```

## Mocking Guidelines

- Use `unittest.mock.Mock` and `unittest.mock.patch` for unit tests
- Mock external dependencies and I/O operations
- Use `patch.object()` for specific method mocking
- Verify mock calls with `assert_called_with()` and `assert_called_once()`

## Coverage Requirements

- **Framework Core**: Minimum 85% coverage
- **Components**: Minimum 90% coverage
- **Middlewares**: Minimum 90% coverage
- **Integration Tests**: All major execution paths
- **Error Handling**: All exception scenarios

## Continuous Integration

The test suite is designed for CI/CD integration:

```bash
# CI test command
pytest --cov=framework --cov=components --cov=middlewares --cov-report=xml --junitxml=test-results.xml

# Performance regression testing
pytest tests/performance/ --benchmark-json=benchmark-results.json
```

## Test Data Management

- Use temporary files and directories for file I/O tests
- Clean up resources in test teardown
- Use fixtures for consistent test data
- Parameterize tests for multiple scenarios

## Debugging Tests

Run tests with debugging:
```bash
# Run with pdb on failure
pytest --pdb

# Run specific test with output
pytest tests/unit/framework/test_service_registry.py::TestServiceRegistry::test_init_with_config -s

# Run with maximum verbosity
pytest -vvv
```

## Contributing Test Cases

When adding new functionality:

1. Write unit tests for new components
2. Add integration tests for new workflows
3. Include performance tests for performance-critical features
4. Update test fixtures with new configuration options
5. Maintain coverage targets

## Common Test Utilities

The test suite includes utilities for:

- Temporary configuration file creation
- Mock component and middleware factories
- Sample data generation
- Performance measurement helpers
- Error scenario simulation