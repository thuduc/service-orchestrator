# Service Pipeline Framework

## Requirements
- Python-based, configuration-driven framework for service execution.
- All components implement a single contract: `execute(context) -> dict`.
- Single service entrypoint that dispatches by `service_id` in the context.
- Steps-based execution model for composing multi-step services.
- Interceptor pipeline for cross-cutting concerns (logging, validation, metrics).
- Support service-scoped interceptors (include/exclude by service).

## Design

### Core Concepts
- **Contract**: `Component` and `Interceptor` interfaces define the public API.
- **Orchestration**: Service registry, steps executor, interceptor pipeline, and entrypoint.
- **Implementation**: Default components and interceptors live in the implementation layer.
- **Configuration**: Services and interceptors are registered via JSON files.

### Execution Flow
1) `ServiceEntrypoint.execute(context)` reads `service_id`.
2) `ServiceRegistry` loads the steps for that service.
3) `StepsExecutor` runs each step sequentially, merging results into the shared context.
4) `InterceptorPipeline` wraps execution with `before/after/on_error` hooks.

### Interceptor Scoping
- Each interceptor can define a `scope` with `include_services` and/or `exclude_services`.
- The pipeline is cached per service to avoid repeated construction.

## Directory Layout
```
frameworks/
  service_pipeline/
    contract/              # interfaces
    orchestration/         # registry, pipelines, executor
    implementation/        # default components + interceptors
    resources/             # services.json, interceptors.json
    tests/                 # unit/integration/performance tests
    main.py                # framework entrypoint
```

## Implementation Details

### Component Contract
```python
class Component(ABC):
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        ...
```

### Interceptor Contract
```python
class Interceptor(ABC):
    def before(self, context: Dict[str, Any]) -> Dict[str, Any]:
        return context

    def after(self, context: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
        return result

    def on_error(self, context: Dict[str, Any], error: Exception) -> Optional[Dict[str, Any]]:
        return None
```

### Configuration
- Services: `frameworks/service_pipeline/resources/services.json`
- Interceptors: `frameworks/service_pipeline/resources/interceptors.json`

Each service defines a list of steps with a `module` and `class`. Example:
```json
{
  "services": {
    "data-processing": {
      "steps": [
        {
          "name": "validate",
          "module": "frameworks.service_pipeline.implementation.components.validation",
          "class": "ValidationComponent",
          "config": {"required_fields": ["data", "request_id"]}
        }
      ]
    }
  }
}
```

### Service-Scoped Interceptors
```json
{
  "interceptors": {
    "validation": {
      "module": "frameworks.service_pipeline.implementation.interceptors.validation",
      "class": "ValidationInterceptor",
      "enabled": true,
      "order": 2,
      "scope": {
        "include_services": ["service-a"],
        "exclude_services": ["service-b"]
      }
    }
  }
}
```

## Setup

### Python Environment
The repo includes a virtual environment under `test_env/`. Use it for testing:
```bash
./test_env/bin/python -m pytest
```

If you need to install test dependencies:
```bash
python3 -m pip install -r requirements-test.txt
```

## Execution

Run the service pipeline demo:
```bash
python3 main.py
```

Or run the framework entrypoint directly:
```bash
python3 frameworks/service_pipeline/main.py
```

## Tests

Run all tests:
```bash
./test_env/bin/python -m pytest
```

Run a specific suite:
```bash
./test_env/bin/python -m pytest frameworks/service_pipeline/tests/unit
```

Performance benchmarks:
```bash
./test_env/bin/python -m pytest frameworks/service_pipeline/tests/performance --benchmark-only
```

## Notes
- The framework is built around step-based orchestration for both single- and multi-step services.
- Interceptors provide cross-cutting concerns with service-level scoping.
- The layout is designed to support additional frameworks alongside `service_pipeline`.
