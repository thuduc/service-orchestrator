## GenAI Proof of Concept: creation of a complex software framework in Python
The purpose of this proof of concept is to find out if an LLM can design and implement a complex framework using foundational software patterns and best practices. For our PoC, we will be using Python as the programming language. The framework we're building is a Service Orchestation framework, which supports dynamic loading of services, service execution, and service orchestration.

### LLM & AI Tool
* LLM used: Claude Opus 4 (best coding LLM) - https://www.anthropic.com/claude/opus
* AI tool used: Claude Code (best coding CLI due to its integration with Clause 4 LLMs) - https://www.anthropic.com/claude-code

### Development Process: 
* Step 1 - use Claude Code to design a Service Orchestration framework using requirements specified in the [REQUIREMENTS.md](REQUIREMENTS.md). The design will be saved to [FRAMEWORK_DESIGN.md](FRAMEWORK_DESIGN.md) file for review.
* Step 2 - developer reviews the FRAMEWORK_DESIGN.md and makes changes as needed. Developer can also provide feedback to Claude Code and iterate until the design is ready.
* Step 3 - use Claude Code to generate code using the [FRAMEWORK_DESIGN.md](FRAMEWORK_DESIGN.md)

### PoC Results
* The process took an hour to complete
* The framework and its default implementation resides under framework/ directory. The sample components reside under components/ directory

### Features

- **Component-based architecture**: All components implement a common `execute(context)` interface
- **Dynamic component loading**: Components are loaded dynamically based on configuration
- **Middleware pipeline**: Support for cross-cutting concerns through middleware
- **Configuration-driven**: Service mappings defined in JSON configuration
- **Extensible design**: Easy to add new components and middleware

### Project Structure

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

### Quick Start

1. Run the example:
```bash
python3 main.py
```

### Creating New Components

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

### Using the Framework

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

### Middleware

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

### Future Enhancements

See `FRAMEWORK_DESIGN.md` for planned features and extensions.