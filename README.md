# Proof-of-concept: creation of a Service Orchestration Framework in Python

This POC is to evaluate Claude Code (an agentic coding tool from Anthropic: https://www.anthropic.com/claude-code), known for its ability to design & build modern software frameworks.

Service Orchestration is a modular Python framework for building component-based microservices with a plugin architecture.

#### Development Process: 
* Step 1 - ask Claude Code to design a Service Orchestration framework using requirements specified in the REQUIREMENTS.md file. We used Anthropic's Claude Opus 4 LLM with Claude Code due to its advanced ability to design and build frameworks. We asked the LLM to save the design to the FRAMEWORK_DESIGN.md file for review.
* Step 2 - developer reviews the FRAMEWORK_DESIGN.md and makes changes as needed.
* Step 3 - ask Claude Code to generate code using the FRAMEWORK_DESIGN.md, including comprehentive using test coverage.

The process took less than 1 hour to complete using Claude Code. 

## Features

- **Component-based architecture**: All components implement a common `execute(context)` interface
- **Dynamic component loading**: Components are loaded dynamically based on configuration
- **Middleware pipeline**: Support for cross-cutting concerns through middleware
- **Configuration-driven**: Service mappings defined in JSON configuration
- **Extensible design**: Easy to add new components and middleware

## Project Structure

```
model-svc/
├── framework/                 # Core framework modules
│   ├── __init__.py
│   ├── component.py          # Base component contract
│   ├── service_registry.py   # Component registration and lookup
│   ├── service_entrypoint.py # Main service entry point
│   ├── middleware.py         # Middleware base classes
│   └── logging_middleware.py # Logging middleware implementation
├── components/               # Component implementations
│   ├── __init__.py
│   ├── pre_calibration.py   # Pre-Calibration component
│   └── simulation.py        # Simulation component
├── services.json            # Service configuration
├── main.py                  # Example usage
└── FRAMEWORK_DESIGN.md      # Detailed design documentation
```

## Quick Start

1. Run the example:
```bash
python3 main.py
```

## Creating New Components

1. Create a new Python module in the `components/` directory
2. Implement the `Component` interface:

```python
from framework.component import Component
from typing import Dict, Any

class MyComponent(Component):
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        # Your component logic here
        return {"status": "success", "message": "Hello from MyComponent"}
```

3. Register the component in `services.json`:

```json
{
  "services": {
    "my-service": {
      "module": "components.my_component",
      "class": "MyComponent",
      "config": {}
    }
  }
}
```

## Using the Framework

```python
from framework import ServiceRegistry, ServiceEntrypoint

# Initialize registry and entrypoint
registry = ServiceRegistry(config_path='services.json')
service = ServiceEntrypoint(registry=registry)

# Execute a component
context = {
    "service_id": "my-service",
    "request_id": "req-123",
    "data": {"key": "value"}
}
result = service.execute(context)
```

## Middleware

The framework supports middleware for cross-cutting concerns:

- **LoggingMiddleware**: Logs component execution details
- Custom middleware can be added by implementing the `Middleware` interface

## Configuration

Services are configured in `services.json`:

```json
{
  "services": {
    "service-id": {
      "module": "module.path",
      "class": "ClassName",
      "config": {
        "param1": "value1"
      }
    }
  }
}
```

## Future Enhancements

See `FRAMEWORK_DESIGN.md` for planned features and extensions.