# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Run the example application
```bash
python3 main.py
```

### Run a specific component
Since this is a pure Python framework with no external dependencies, components can be tested individually by creating a test script that imports and executes them.

## Architecture Overview

This is a **Model Service Framework** - a modular Python framework for building component-based microservices with a plugin architecture. The key architectural concepts are:

### Core Framework Components

1. **Component Contract** (`framework/component.py`): All components implement a common `execute(context)` interface. This is the fundamental building block.

2. **Service Registry** (`framework/service_registry.py`): Manages dynamic component loading and lookup based on configuration in `services.json`. Components are loaded on-demand using Python's import machinery.

3. **Service Entrypoint** (`framework/service_entrypoint.py`): The main execution entry point that:
   - Extracts the `service_id` from the execution context
   - Retrieves the appropriate component from the registry
   - Executes the component through the middleware pipeline

4. **Middleware Pipeline** (`framework/middleware.py`): Implements a chain of responsibility pattern for cross-cutting concerns. Middleware can intercept execution before and after component processing.

### Component Implementation Pattern

Components live in the `components/` directory and must:
- Inherit from `framework.component.Component`
- Implement the `execute(self, context: Dict[str, Any]) -> Dict[str, Any]` method
- Be registered in `services.json` with their module path and class name

Example component structure:
```python
from framework.component import Component
from typing import Dict, Any

class MyComponent(Component):
    def __init__(self, config: Dict[str, Any]):
        self.config = config
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        # Component logic here
        return {"status": "success", "result": ...}
```

### Service Configuration

Services are configured in `services.json`:
```json
{
  "services": {
    "service-id": {
      "module": "components.module_name",
      "class": "ComponentClassName",
      "config": {
        // Component-specific configuration
      }
    }
  }
}
```

### Execution Flow

1. Context object with `service_id` is passed to `ServiceEntrypoint.execute()`
2. Service ID is extracted from context
3. Component is dynamically loaded from registry
4. Middleware pipeline processes the request
5. Component executes with the provided context
6. Result flows back through middleware
7. Final result is returned

### Current Components

- **PreCalibrationComponent** (`components/pre_calibration.py`): Handles pre-calibration logic
- **SimulationComponent** (`components/simulation.py`): Handles simulation logic

### Adding New Components

1. Create a new file in `components/` directory
2. Implement the `Component` interface
3. Add entry to `services.json`
4. Component is automatically available through the framework

The framework is designed for extensibility - new middleware, components, and cross-cutting concerns can be added without modifying the core framework code.