# Architectural Enhancement: Multi-Component Services

## Executive Summary

This document outlines a refactoring plan to transform the Model Service Framework to exclusively use a steps-based architecture where all services are composed of one or more components that execute sequentially. This transformation will provide a consistent, unified approach to service definition while maintaining the framework's modularity and extensibility.

## Current State Analysis

### Limitations
- **One-to-One Mapping**: Each service ID maps to exactly one component
- **Single Execution Path**: No native support for component chaining or pipelines
- **Limited Composition**: Complex workflows require custom component implementations

### Existing Strengths to Preserve
- Clean component contract with `execute(context)` interface
- Middleware pipeline for cross-cutting concerns
- Dynamic component loading via service registry
- Configuration-driven architecture

## Proposed Architecture

### Core Concept: Unified Steps-Based Architecture

All services are configured using a steps-based approach:
- **Every service** uses the `steps` array configuration
- Services with single components have one step
- Services with multiple components have multiple steps
- Consistent interface and execution model for all services

### Configuration Schema

#### Unified Steps-Based Configuration
All services use a `steps` array where each service name (e.g., "order-processing", "simple-calculation") defines one or more sequential components:

```json
{
  "services": {
    "order-processing": {
      "steps": [
        {
          "name": "validate",
          "module": "components.validation",
          "class": "ValidationComponent",
          "config": {...},
          "output_mapping": {
            "validated_data": "data"
          }
        },
        {
          "name": "transform",
          "module": "components.transformation",
          "class": "TransformationComponent",
          "config": {...},
          "input_mapping": {
            "data": "validated_data"
          },
          "output_mapping": {
            "transformed_data": "processed"
          }
        },
        {
          "name": "persist",
          "module": "components.persistence",
          "class": "PersistenceComponent",
          "config": {...},
          "input_mapping": {
            "processed": "transformed_data"
          }
        }
      ]
    },
    "simple-calculation": {
      "steps": [
        {
          "name": "calculate",
          "module": "components.calculator",
          "class": "CalculatorComponent",
          "config": {...}
        }
      ]
    }
  }
}
```

Key features:
- **Service Name**: Each top-level key (e.g., "order-processing") represents a unique service identifier
- **Steps Array**: Required for all services, defines the sequence of components to execute
- **Step Name**: Unique identifier within the service for logging and debugging
- **Input/Output Mapping**: Controls data flow between steps (optional for single-step services)
- **Consistency**: All services use the same configuration structure

## Refactoring Plan

### Phase 1: Foundation

#### 1.1 Create Steps Executor
**New File**: `framework/steps_executor.py`
- Implements sequential step execution for multi-step services
- Manages data flow between steps using input/output mappings
- Handles error propagation and step-level error handling
- Provides execution context tracking per step

#### 1.2 Replace Service Registry
**Modify**: `framework/service_registry.py`
- Replace existing registry with steps-based service loader
- Require `steps` array for all service definitions
- Validate that all services have at least one step
- Create unified factory method for service instantiation

#### 1.3 Create Step Context Manager
**New File**: `framework/step_context.py`
- Manages execution context across steps
- Implements input/output mapping transformations
- Tracks step execution state and results
- Provides step-level context isolation

### Phase 2: Core Integration

#### 2.1 Replace Service Entrypoint
**Modify**: `framework/service_entrypoint.py`
- Replace existing entrypoint with steps-based execution
- Always delegate to StepsExecutor
- Remove legacy single component execution path
- Maintain external API contract

#### 2.2 Configuration Validator
**New File**: `framework/config_validator.py`
- Validate steps-based service configurations at startup
- Ensure all step components can be loaded
- Validate input/output mappings between steps
- Check for missing required step fields

### Phase 3: Advanced Features

#### 3.1 Conditional Execution
- Add support for conditional step execution
- Implement branching logic based on previous step results

#### 3.2 Parallel Execution
- Support parallel component execution where dependencies allow
- Implement fork-join patterns

#### 3.3 Error Handling Strategies
- Configurable retry policies per step
- Compensation/rollback mechanisms
- Circuit breaker patterns

## Implementation Details

### Data Flow with Input/Output Mappings

The steps-based configuration uses explicit input/output mappings to control data flow:

#### Step Execution Context
Each step maintains its own execution context with:
- **Input**: Data mapped from previous steps based on `input_mapping`
- **Output**: Results mapped to context based on `output_mapping`
- **Global Context**: Shared context accessible to all steps

#### Mapping Example
```json
{
  "steps": [
    {
      "name": "validate",
      "output_mapping": {
        "validated_data": "data",
        "validation_errors": "errors"
      }
    },
    {
      "name": "transform",
      "input_mapping": {
        "data": "validated_data"
      },
      "output_mapping": {
        "transformed_result": "result"
      }
    }
  ]
}
```

#### Context Evolution
```python
# Initial context
context = {"user_id": "123", "request_data": {...}}

# After step 1 (validate)
context = {
    "user_id": "123",
    "request_data": {...},
    "validated_data": {...},  # Added by output_mapping
    "validation_errors": []    # Added by output_mapping
}

# Step 2 receives filtered input based on input_mapping
step2_input = {
    "data": context["validated_data"]  # Mapped from context
}
```

### Error Handling

#### Step Failure Modes
1. **Fail Fast**: Stop on first step error (default)
2. **Fail Safe**: Continue with degraded functionality, skip failed step
3. **Compensate**: Execute rollback for completed steps

#### Step-Level Error Configuration
```json
{
  "steps": [
    {
      "name": "validate",
      "on_error": "fail_fast"
    },
    {
      "name": "enrich",
      "on_error": "skip",
      "fallback_output": {
        "enriched_data": null
      }
    }
  ]
}
```

### Middleware Integration

Middleware continues to wrap the entire service execution:
```
Middleware -> StepsExecutor -> Step1 -> Step2 -> Step3
```

Each step in the service benefits from middleware processing, ensuring consistent behavior across all services.

## Migration Strategy

### Step 1: Convert Existing Services
- Create migration script to convert existing single-component services to single-step services
- Wrap each existing component in a steps array with a single step
- Validate all converted configurations

### Step 2: Deploy New Architecture
- Replace core framework components with steps-based implementation
- Remove support for legacy single-component configuration
- Deploy with converted service configurations

### Step 3: Optimize and Refactor
- Identify services that can benefit from multi-step decomposition
- Refactor complex single-step services into logical multi-step workflows
- Create reusable step component library

## Testing Strategy

### Unit Tests
- Test steps executor with mock components
- Validate input/output mapping transformations
- Test step-level error handling
- Verify step execution order

### Integration Tests
- Test single-step services (converted from legacy)
- Test multi-step services with real components
- Test middleware interaction with steps executor
- Validate context isolation between steps

### Performance Tests
- Measure overhead of steps-based execution for single-step services
- Compare with legacy direct component calls (baseline)
- Test with various numbers of steps (1, 3, 5, 10+ steps)
- Benchmark input/output mapping overhead

## Risks and Mitigations

### Risk 1: Performance Overhead
**Mitigation**: Implement lazy loading and optimize context passing

### Risk 2: Complex Debugging
**Mitigation**: Add comprehensive step-level logging with step names and context snapshots

### Risk 3: Breaking Changes
**Mitigation**: Provide automated migration script to convert existing configurations

## Success Metrics

1. **Consistency**: 100% of services use steps-based configuration
2. **Performance**: < 5% overhead for single-step services vs legacy
3. **Simplicity**: Single unified execution model for all services
4. **Maintainability**: Reduced framework complexity by 40%
5. **Developer Experience**: Consistent configuration and debugging across all services

## Timeline Estimate

- **Phase 1**: 2-3 days
- **Phase 2**: 2-3 days
- **Phase 3**: 3-5 days
- **Testing & Documentation**: 2-3 days

**Total**: 2-3 weeks for full implementation

## Alternative Approaches Considered

### 1. Workflow Engine Integration
- Pros: Full workflow capabilities, industry standard
- Cons: Heavy dependency, overkill for simple pipelines

### 2. Component Composition Pattern
- Pros: Pure OOP approach, no framework changes
- Cons: Verbose, loses configuration-driven benefits

### 3. Event-Driven Architecture
- Pros: Loose coupling, scalable
- Cons: Complex for sequential workflows, debugging challenges

## Conclusion

The unified steps-based architecture simplifies the framework by providing a single, consistent model for all services. Every service, whether simple or complex, uses the same configuration structure and execution path. This eliminates special cases, reduces code complexity, and provides a clear mental model for developers. The migration strategy ensures a smooth transition from the legacy architecture while the new design positions the framework for future enhancements like conditional execution and parallel steps.