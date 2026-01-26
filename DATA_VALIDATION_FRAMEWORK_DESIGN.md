# Data Validation Framework Design Document

## Overview

The Data Validation Framework is a configuration-driven, multi-stage validation engine built on top of **Pandera** with **Polars** as the DataFrame backend. It provides a flexible pipeline architecture for validating data through multiple validation stages, with support for custom checks that can be registered separately.

### Key Design Principles

1. **Configuration-Driven**: Validation pipelines are defined in JSON/YAML configuration files
2. **Multi-Stage Validation**: Support for complex validation workflows with multiple stages
3. **Pandera-Powered**: Leverages Pandera's schema validation for Polars DataFrames
4. **Extensible**: Custom checks can be defined and registered separately
5. **Dual-Use**: Components can be used standalone or as steps in the Service Pipeline Framework

---

## Architecture

```
frameworks/data_validation/
├── __init__.py                          # Public API exports
├── contract/
│   ├── __init__.py
│   ├── check.py                         # CustomCheck ABC
│   ├── validator.py                     # Validator ABC
│   └── validation_stage.py              # ValidationStage ABC
├── engine/
│   ├── __init__.py
│   ├── validation_engine.py             # Main orchestration engine
│   ├── validation_context.py            # Context passed through stages
│   ├── validation_result.py             # Result aggregation
│   └── config_validator.py              # Configuration validation
├── stages/
│   ├── __init__.py
│   ├── base_stage.py                    # Base stage implementation
│   ├── schema_validation.py             # Pandera schema validation stage
│   ├── custom_rules.py                  # Custom rule execution stage
│   ├── cross_field_validation.py        # Multi-column validation stage
│   └── referential_validation.py        # Cross-dataset validation stage
├── loaders/
│   ├── __init__.py
│   ├── config_loader.py                 # Unified config loader
│   ├── json_loader.py                   # JSON config parser
│   └── yaml_loader.py                   # YAML config parser
├── registries/
│   ├── __init__.py
│   ├── check_registry.py                # Custom check registry
│   └── stage_registry.py                # Stage type registry
├── adapters/
│   ├── __init__.py
│   └── pipeline_adapter.py              # Service Pipeline adapter component
├── checks/
│   ├── __init__.py
│   ├── base_check.py                    # Base class for custom checks
│   └── builtin/                         # Built-in custom checks
│       ├── __init__.py
│       ├── string_checks.py             # String validation checks
│       ├── numeric_checks.py            # Numeric validation checks
│       ├── date_checks.py               # Date/time validation checks
│       └── business_checks.py           # Common business rule checks
├── resources/
│   ├── validation_pipelines.json        # Example pipeline configurations
│   └── custom_checks.json               # Example custom check definitions
└── tests/
    ├── unit/
    ├── integration/
    └── fixtures/
```

---

## Core Components

### 1. Validation Engine

The `ValidationEngine` is the main orchestration component that:
- Loads validation pipeline configurations
- Manages the check registry
- Executes validation stages in sequence
- Aggregates results from all stages

```python
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
import polars as pl

class ValidationEngine:
    """
    Main orchestration engine for validation pipelines.
    
    Note: Method signatures shown here define the public API. 
    See Section 4 "Data Propagation Model" for the authoritative validate() implementation.
    """
    
    def __init__(
        self,
        pipeline_config_path: Optional[str] = None,
        check_config_path: Optional[str] = None
    ):
        """
        Initialize the validation engine.
        
        Args:
            pipeline_config_path: Path to validation pipelines JSON/YAML config
            check_config_path: Path to custom checks JSON/YAML config
        """
        self._pipelines: Dict[str, Any] = {}
        self._check_registry = CheckRegistry()
        self._stage_registry = StageRegistry()
        # Load configs and register built-in stages...
    
    def validate(
        self,
        pipeline_id: str,
        data: pl.DataFrame,
        context: Optional[Dict[str, Any]] = None
    ) -> "ValidationResult":
        """
        Execute a validation pipeline on the given data.
        
        Args:
            pipeline_id: ID of the validation pipeline to execute
            data: Polars DataFrame to validate
            context: Optional context data (e.g., reference datasets)
            
        Returns:
            ValidationResult containing all stage results
            
        See Section 4 "Engine Data Flow Implementation" for the full implementation.
        """
        ...
    
    def register_check(self, check_id: str, check_class: type) -> None:
        """Register a custom check class."""
        self._check_registry.register(check_id, check_class)
    
    def list_pipelines(self) -> List[str]:
        """List all registered validation pipelines."""
        return list(self._pipelines.keys())
```

### 2. Validation Context

The `ValidationContext` carries data and metadata through validation stages:

```python
@dataclass
class ValidationContext:
    """Context passed through validation stages."""
    
    data: pl.DataFrame                           # Primary data being validated (mutable)
    reference_data: Dict[str, pl.DataFrame]      # Reference datasets for lookups
    metadata: Dict[str, Any]                     # Additional metadata
    stage_results: List["StageResult"]           # Results from completed stages
    current_stage: Optional[str]                 # Currently executing stage
    pipeline_id: str                             # ID of the pipeline being executed
    
    def add_stage_result(self, result: "StageResult") -> None:
        """Add a stage result to the context."""
        self.stage_results.append(result)
    
    def get_validated_data(self) -> pl.DataFrame:
        """Get the data after all transformations/coercions."""
        return self.data
```

### 3. Validation Result

The `ValidationResult` aggregates results from all validation stages:

```python
@dataclass
class ValidationResult:
    """Aggregated result from a validation pipeline."""
    
    pipeline_id: str
    is_valid: bool
    stage_results: List["StageResult"]
    total_errors: int
    total_warnings: int
    rows_validated: int                           # Total rows processed (final context.data.height)
    execution_time_ms: float
    validated_data: Optional[pl.DataFrame]       # Data after coercion (if applicable)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "pipeline_id": self.pipeline_id,
            "is_valid": self.is_valid,
            "total_errors": self.total_errors,
            "total_warnings": self.total_warnings,
            "rows_validated": self.rows_validated,
            "execution_time_ms": self.execution_time_ms,
            "stages": [
                {
                    "name": s.stage_name,
                    "type": s.stage_type,
                    "is_valid": s.is_valid,
                    "errors": [e.__dict__ for e in s.errors],
                    "warnings": [w.__dict__ for w in s.warnings],
                }
                for s in self.stage_results
            ],
        }
    
    def get_errors_by_stage(self) -> Dict[str, List["ValidationError"]]:
        """Get errors grouped by stage."""
        return {
            stage.stage_name: stage.errors 
            for stage in self.stage_results 
            if stage.errors
        }

@dataclass
class StageResult:
    """Result from a single validation stage."""
    
    stage_name: str
    stage_type: str
    is_valid: bool
    errors: List["ValidationError"]
    warnings: List["ValidationWarning"]
    execution_time_ms: float
    rows_validated: int
    rows_failed: int
    output_data: Optional[pl.DataFrame] = None    # Transformed data (only for coercing stages)

@dataclass
class ValidationError:
    """Individual validation error."""
    
    check_name: str
    column: Optional[str]
    error_message: str
    failure_cases: Optional[List[Any]]           # Sample of failing values
    row_indices: Optional[List[int]]             # Indices of failing rows

@dataclass
class ValidationWarning:
    """Individual validation warning (non-fatal issue)."""
    
    check_name: str
    column: Optional[str]
    message: str
    row_count: int = 0                            # Number of affected rows (for dropped rows)
```

### 4. Data Propagation Model

This section defines how data flows through validation stages and how validated/coerced data is tracked.

#### Data Flow Rules

1. **Context Data Mutation**: The `ValidationContext.data` field is mutable. Stages that perform coercion (e.g., `schema_validation` with `coerce=true`) update `context.data` with the transformed DataFrame.

2. **Stage Output Data**: `StageResult.output_data` is populated only by stages that transform data. Most stages are read-only validators and leave `output_data` as `None`.

3. **Final Validated Data**: `ValidationResult.validated_data` is set to `context.data` after all stages complete. This reflects all coercions/transformations applied during the pipeline.

#### Stage Data Behavior

| Stage Type | Modifies Data? | `output_data` Set? |
|------------|----------------|-------------------|
| `schema_validation` (coerce=false) | No | No |
| `schema_validation` (coerce=true) | Yes | Yes |
| `custom_rules` | No | No |
| `cross_field_validation` | No | No |
| `referential_validation` | No | No |

#### Engine Data Flow Implementation

```python
class ValidationEngine:
    def validate(
        self,
        pipeline_id: str,
        data: pl.DataFrame,
        context: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        """Execute validation pipeline with proper data propagation."""
        import time
        
        pipeline_start = time.perf_counter()
        
        # Initialize context with input data
        ctx = ValidationContext(
            data=data.clone(),  # Clone to avoid mutating input
            reference_data=context.get("reference_data", {}) if context else {},
            metadata=context.get("metadata", {}) if context else {},
            stage_results=[],
            current_stage=None,
            pipeline_id=pipeline_id,
        )
        
        pipeline_config = self._pipelines[pipeline_id]
        on_failure = pipeline_config.get("on_failure", "collect_all")
        
        for stage_config in pipeline_config["stages"]:
            ctx.current_stage = stage_config["name"]
            stage = self._create_stage(stage_config)
            
            # Execute stage with timing
            stage_start = time.perf_counter()
            result = stage.execute(ctx)
            result.execution_time_ms = (time.perf_counter() - stage_start) * 1000
            
            # Update context data if stage produced output
            if result.output_data is not None:
                ctx.data = result.output_data
            
            ctx.add_stage_result(result)
            
            # Check pipeline-level failure behavior
            if not result.is_valid and on_failure == "fail_fast":
                break
        
        # Aggregate results
        total_execution_ms = (time.perf_counter() - pipeline_start) * 1000
        
        return ValidationResult(
            pipeline_id=pipeline_id,
            is_valid=all(r.is_valid for r in ctx.stage_results),
            stage_results=ctx.stage_results,
            total_errors=sum(len(r.errors) for r in ctx.stage_results),
            total_warnings=sum(len(r.warnings) for r in ctx.stage_results),
            rows_validated=len(ctx.data),  # Final row count after any transformations
            execution_time_ms=total_execution_ms,
            validated_data=ctx.data,  # Final data after all transformations
        )
```

#### Updated SchemaValidationStage with Data Output

```python
def execute(self, context: ValidationContext) -> StageResult:
    """Execute schema validation with proper data propagation."""
    import time
    start = time.perf_counter()
    
    try:
        validated_df = self._schema.validate(context.data, lazy=True)
        execution_ms = (time.perf_counter() - start) * 1000
        
        return StageResult(
            stage_name=self.name,
            stage_type=self.stage_type,
            is_valid=True,
            errors=[],
            warnings=[],
            execution_time_ms=execution_ms,
            rows_validated=len(context.data),
            rows_failed=0,
            # Output coerced data if coercion was enabled
            output_data=validated_df if self._config.get("coerce", False) else None,
        )
    except pa.errors.SchemaErrors as exc:
        errors = self._parse_pandera_errors(exc)
        execution_ms = (time.perf_counter() - start) * 1000
        
        return StageResult(
            stage_name=self.name,
            stage_type=self.stage_type,
            is_valid=False,
            errors=errors,
            warnings=[],
            execution_time_ms=execution_ms,
            rows_validated=len(context.data),
            rows_failed=len(set(idx for e in errors if e.row_indices for idx in e.row_indices)),
            output_data=None,  # No output on failure
        )
```

#### Drop Invalid Rows Behavior

The `drop_invalid_rows` config option controls whether failing rows are removed from the output data. Since Pandera for Polars doesn't automatically drop rows, this behavior is implemented explicitly.

The `treat_dropped_as_failure` config option controls whether the stage is considered failed even after dropping invalid rows. This is useful when you want to drop bad rows for downstream processing but still want the pipeline to respect `on_failure=fail_fast` behavior.

**Behavior Matrix**:

| `drop_invalid_rows` | `treat_dropped_as_failure` | Validation Outcome | `is_valid` | Errors/Warnings | `output_data` |
|---------------------|---------------------------|-------------------|------------|-----------------|---------------|
| `false` (default) | N/A | Pass | `true` | None | Coerced data (if `coerce=true`) or `None` |
| `false` (default) | N/A | Fail | `false` | Errors | `None` |
| `true` | `false` (default) | Pass | `true` | None | Coerced data (if `coerce=true`) or `None` |
| `true` | `false` (default) | Fail | `true` | Warnings (converted from errors) | Data with failing rows removed |
| `true` | `true` | Pass | `true` | None | Coerced data (if `coerce=true`) or `None` |
| `true` | `true` | Fail | `false` | Errors (not converted) | Data with failing rows removed |

**Key Points**:
- When `drop_invalid_rows=true` and `treat_dropped_as_failure=false` (default): The stage is marked as valid even when rows are dropped, and errors are converted to warnings. This makes `on_failure=fail_fast` a no-op for this stage.
- When `drop_invalid_rows=true` and `treat_dropped_as_failure=true`: The stage is marked as invalid when rows are dropped, errors remain as errors, but the data still has failing rows removed. This allows the pipeline to stop via `on_failure=fail_fast` while still providing cleaned data in `output_data`.

**Implementation**:

```python
def execute(self, context: ValidationContext) -> StageResult:
    """Execute schema validation with drop_invalid_rows support."""
    import time
    start = time.perf_counter()
    drop_invalid = self._config.get("drop_invalid_rows", False)
    treat_dropped_as_failure = self._config.get("treat_dropped_as_failure", False)
    
    try:
        validated_df = self._schema.validate(context.data, lazy=True)
        execution_ms = (time.perf_counter() - start) * 1000
        
        return StageResult(
            stage_name=self.name,
            stage_type=self.stage_type,
            is_valid=True,
            errors=[],
            warnings=[],
            execution_time_ms=execution_ms,
            rows_validated=len(context.data),
            rows_failed=0,
            output_data=validated_df if self._config.get("coerce", False) else None,
        )
    except pa.errors.SchemaErrors as exc:
        errors = self._parse_pandera_errors(exc)
        execution_ms = (time.perf_counter() - start) * 1000
        
        if drop_invalid:
            # Collect all failing row indices
            failing_indices = set()
            for error in errors:
                if error.row_indices:
                    failing_indices.update(error.row_indices)
            
            # Remove failing rows from data
            all_indices = set(range(len(context.data)))
            valid_indices = sorted(all_indices - failing_indices)
            cleaned_df = context.data[valid_indices]
            
            if treat_dropped_as_failure:
                # Keep errors as errors, stage is invalid, but still provide cleaned data
                return StageResult(
                    stage_name=self.name,
                    stage_type=self.stage_type,
                    is_valid=False,  # Stage failed - respects on_failure=fail_fast
                    errors=errors,   # Keep as errors
                    warnings=[],
                    execution_time_ms=execution_ms,
                    rows_validated=len(context.data),
                    rows_failed=len(failing_indices),
                    output_data=cleaned_df,  # Still provide cleaned data
                )
            else:
                # Convert errors to warnings since we're dropping the bad rows
                warnings = [
                    ValidationWarning(
                        check_name=e.check_name,
                        column=e.column,
                        message=f"[DROPPED] {e.error_message}",
                        row_count=len(e.row_indices) if e.row_indices else 0,
                    )
                    for e in errors
                ]
                
                return StageResult(
                    stage_name=self.name,
                    stage_type=self.stage_type,
                    is_valid=True,  # Valid because bad rows were dropped
                    errors=[],
                    warnings=warnings,
                    execution_time_ms=execution_ms,
                    rows_validated=len(context.data),
                    rows_failed=len(failing_indices),
                    output_data=cleaned_df,  # Cleaned data without failing rows
                )
        else:
            return StageResult(
                stage_name=self.name,
                stage_type=self.stage_type,
                is_valid=False,
                errors=errors,
                warnings=[],
                execution_time_ms=execution_ms,
                rows_validated=len(context.data),
                rows_failed=len(set(idx for e in errors if e.row_indices for idx in e.row_indices)),
                output_data=None,
            )
```

**Use Case**: `drop_invalid_rows=true` is useful for data pipelines where you want to:
1. Process valid records and continue the pipeline
2. Log/report invalid records as warnings
3. Avoid pipeline failures due to a small number of bad records

### 5. Failure Behavior Model

This section clarifies how failure handling works at both the pipeline and stage levels.

#### Two-Level Failure Control

| Level | Setting | Controls |
|-------|---------|----------|
| **Pipeline** | `on_failure` | Whether to continue to the **next stage** after a stage fails |
| **Stage** | `fail_fast` | Whether to stop executing **rules within** the stage after first rule failure |

These are orthogonal controls that operate at different levels:

```
Pipeline Level:  [Stage 1] → [Stage 2] → [Stage 3]   ← on_failure controls this
                     ↓
Stage Level:     [Rule A] → [Rule B] → [Rule C]      ← fail_fast controls this
```

#### Pipeline `on_failure` Behavior

| Value | Behavior |
|-------|----------|
| `collect_all` (default) | Execute all stages regardless of failures. Collect all errors. |
| `fail_fast` | Stop pipeline execution after first stage that fails (`is_valid=false`). |

#### Stage `fail_fast` Behavior

| Value | Behavior |
|-------|----------|
| `false` (default) | Execute all rules in the stage. Collect all rule errors. |
| `true` | Stop executing rules after first rule failure within the stage. |

#### Example Scenarios

**Scenario 1**: `on_failure: collect_all`, stage `fail_fast: false`
- Stage 1 fails with 3 errors → continue to Stage 2
- Stage 2 fails with 2 errors → continue to Stage 3
- Stage 3 passes → pipeline complete with 5 total errors

**Scenario 2**: `on_failure: fail_fast`, stage `fail_fast: false`  
- Stage 1 fails with 3 errors → pipeline stops
- Only Stage 1 errors are reported (3 errors)

**Scenario 3**: `on_failure: collect_all`, stage `fail_fast: true`
- Stage 1 has 3 rules, first rule fails → Stage 1 stops (1 error), continue to Stage 2
- Stage 2 passes → continue to Stage 3
- Stage 3 has 2 rules, both pass → pipeline complete with 1 error

#### Engine Implementation

```python
class ValidationEngine:
    def validate(self, pipeline_id: str, data: pl.DataFrame, ...) -> ValidationResult:
        # ... setup ...
        
        pipeline_config = self._pipelines[pipeline_id]
        on_failure = pipeline_config.get("on_failure", "collect_all")
        
        for stage_config in pipeline_config["stages"]:
            stage = self._create_stage(stage_config)
            result = stage.execute(ctx)
            ctx.add_stage_result(result)
            
            # Pipeline-level failure control
            if not result.is_valid and on_failure == "fail_fast":
                break  # Stop executing further stages
        
        # ... aggregate and return ...
```

### 6. Config Validator

The `ConfigValidator` provides pre-execution validation of pipeline and check configurations. This ensures that configuration errors are caught early (at load time) rather than during validation execution.

```python
from dataclasses import dataclass
from typing import Dict, Any, List, Optional


@dataclass
class ConfigValidationError:
    """Represents a configuration validation error."""
    
    path: str                   # JSON path to the error location (e.g., "pipelines.customer_validation.stages[0].config")
    message: str                # Human-readable error message
    severity: str               # "error" or "warning"


class ConfigValidator:
    """
    Validates pipeline and check configurations before execution.
    
    This validator ensures:
    - Required fields are present
    - Field values are of the correct type
    - Referenced checks exist in the registry
    - Referenced stage types are valid
    - Pipeline structure is correct
    """
    
    def __init__(self, check_registry: "CheckRegistry", stage_registry: "StageRegistry"):
        """
        Initialize the config validator.
        
        Args:
            check_registry: Registry of available custom checks
            stage_registry: Registry of available stage types
        """
        self._check_registry = check_registry
        self._stage_registry = stage_registry
    
    def validate_pipeline_config(
        self, 
        config: Dict[str, Any]
    ) -> List[ConfigValidationError]:
        """
        Validate a pipeline configuration.
        
        Checks:
        - Pipeline has required fields (stages array)
        - Each stage has required fields (name, type, config)
        - Stage types are registered
        - Stage-specific config is valid (see Stage-Specific Validation Rules below)
        
        Args:
            config: Pipeline configuration dictionary
            
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        # Validate pipeline has stages
        if "stages" not in config or not config["stages"]:
            errors.append(ConfigValidationError(
                path="stages",
                message="Pipeline must have at least one stage",
                severity="error"
            ))
            return errors
        
        # Validate each stage
        for i, stage in enumerate(config["stages"]):
            stage_path = f"stages[{i}]"
            
            # Required fields
            for field in ["name", "type", "config"]:
                if field not in stage:
                    errors.append(ConfigValidationError(
                        path=f"{stage_path}.{field}",
                        message=f"Stage is missing required field '{field}'",
                        severity="error"
                    ))
            
            if "type" in stage:
                # Validate stage type is registered
                if not self._stage_registry.has(stage["type"]):
                    errors.append(ConfigValidationError(
                        path=f"{stage_path}.type",
                        message=f"Unknown stage type '{stage['type']}'",
                        severity="error"
                    ))
                elif "config" in stage:
                    # Validate stage-specific config
                    errors.extend(self._validate_stage_config(
                        stage["type"], 
                        stage["config"], 
                        f"{stage_path}.config"
                    ))
        
        return errors
    
    def _validate_stage_config(
        self,
        stage_type: str,
        config: Dict[str, Any],
        path: str
    ) -> List[ConfigValidationError]:
        """Validate stage-specific configuration. See Stage-Specific Validation Rules."""
        errors = []
        rules = STAGE_VALIDATION_RULES.get(stage_type, {})
        
        # Check required fields
        for field in rules.get("required_fields", []):
            if field not in config:
                errors.append(ConfigValidationError(
                    path=f"{path}.{field}",
                    message=f"Stage type '{stage_type}' requires field '{field}'",
                    severity="error"
                ))
            elif rules.get("non_empty_fields") and field in rules["non_empty_fields"]:
                if not config[field]:
                    errors.append(ConfigValidationError(
                        path=f"{path}.{field}",
                        message=f"Field '{field}' cannot be empty for stage type '{stage_type}'",
                        severity="error"
                    ))
        
        # Check rule-level requirements
        if "rules" in config and "rule_requirements" in rules:
            for i, rule in enumerate(config["rules"]):
                for req_field in rules["rule_requirements"]:
                    if req_field not in rule:
                        errors.append(ConfigValidationError(
                            path=f"{path}.rules[{i}].{req_field}",
                            message=f"Rule in '{stage_type}' stage requires field '{req_field}'",
                            severity="error"
                        ))
        
        return errors
    
    def validate_check_config(
        self, 
        config: Dict[str, Any]
    ) -> List[ConfigValidationError]:
        """
        Validate a custom check configuration.
        
        Checks:
        - Check has required fields (module, class)
        - Module path is valid format
        - Default params match expected types (if schema provided)
        
        Args:
            config: Check configuration dictionary
            
        Returns:
            List of validation errors (empty if valid)
        """
        pass
    
    def validate_rule_config(
        self, 
        rule: Dict[str, Any],
        stage_type: str
    ) -> List[ConfigValidationError]:
        """
        Validate a rule configuration within a stage.
        
        Checks:
        - Rule has required fields for the stage type
        - Referenced check_id exists in registry
        - Params match check's expected parameters
        
        Args:
            rule: Rule configuration dictionary
            stage_type: Type of the parent stage
            
        Returns:
            List of validation errors (empty if valid)
        """
        pass
    
    def validate_all(
        self,
        pipelines_config: Dict[str, Any],
        checks_config: Optional[Dict[str, Any]] = None
    ) -> List[ConfigValidationError]:
        """
        Validate all configurations.
        
        This is typically called during ValidationEngine initialization
        to catch configuration errors early.
        
        Args:
            pipelines_config: Full pipelines configuration file content
            checks_config: Full checks configuration file content (optional)
            
        Returns:
            List of all validation errors across all configs
        """
        pass
```

#### Config Validation Error Examples

```python
# Example errors that ConfigValidator can detect:

errors = [
    ConfigValidationError(
        path="validation_pipelines.customer_validation.stages[1].config.rules[0]",
        message="Referenced check 'invalid_check_id' not found in check registry",
        severity="error"
    ),
    ConfigValidationError(
        path="validation_pipelines.order_validation.stages[0].config.columns.price",
        message="Unknown dtype 'Integer' - did you mean 'Int64'?",
        severity="error"
    ),
    ConfigValidationError(
        path="validation_pipelines.product_validation",
        message="Pipeline has no stages defined",
        severity="error"
    ),
    ConfigValidationError(
        path="custom_checks.my_check.module",
        message="Module 'myproject.checks.invalid' could not be found",
        severity="warning"  # Warning because module might be available at runtime
    ),
]
```

#### Stage-Specific Validation Rules

The `ConfigValidator` applies type-specific validation rules for each stage type. These rules ensure that required fields are present and properly structured.

**Rules Matrix**:

| Stage Type | Required Fields | Non-Empty Fields | Rule-Level Requirements |
|------------|-----------------|------------------|-------------------------|
| `schema_validation` | `columns` | `columns` | N/A (no rules array) |
| `custom_rules` | `rules` | `rules` | `check_id` |
| `cross_field_validation` | `rules` | `rules` | `check_id`, `columns` |
| `referential_validation` | `rules` | `rules` | `check_id`, `reference_dataset`, `reference_column` |

**Implementation**:

```python
# Stage-specific validation rules configuration
STAGE_VALIDATION_RULES = {
    "schema_validation": {
        "required_fields": ["columns"],
        "non_empty_fields": ["columns"],
    },
    "custom_rules": {
        "required_fields": ["rules"],
        "non_empty_fields": ["rules"],
        "rule_requirements": ["check_id"],
    },
    "cross_field_validation": {
        "required_fields": ["rules"],
        "non_empty_fields": ["rules"],
        "rule_requirements": ["check_id", "columns"],
    },
    "referential_validation": {
        "required_fields": ["rules"],
        "non_empty_fields": ["rules"],
        "rule_requirements": ["check_id", "reference_dataset", "reference_column"],
    },
}
```

**Example Validation Errors**:

```python
# Missing columns in schema_validation
ConfigValidationError(
    path="pipelines.customer_validation.stages[0].config.columns",
    message="Stage type 'schema_validation' requires field 'columns'",
    severity="error"
)

# Empty rules array in custom_rules
ConfigValidationError(
    path="pipelines.customer_validation.stages[1].config.rules",
    message="Field 'rules' cannot be empty for stage type 'custom_rules'",
    severity="error"
)

# Missing reference_dataset in referential_validation rule
ConfigValidationError(
    path="pipelines.order_validation.stages[2].config.rules[0].reference_dataset",
    message="Rule in 'referential_validation' stage requires field 'reference_dataset'",
    severity="error"
)

# Missing columns in cross_field_validation rule
ConfigValidationError(
    path="pipelines.order_validation.stages[1].config.rules[0].columns",
    message="Rule in 'cross_field_validation' stage requires field 'columns'",
    severity="error"
)
```

#### Integration with ValidationEngine

The `ConfigValidator` is automatically invoked during `ValidationEngine` initialization:

```python
class ValidationEngine:
    def __init__(
        self,
        pipeline_config_path: Optional[str] = None,
        check_config_path: Optional[str] = None,
        validate_config: bool = True  # Enable/disable pre-validation
    ):
        # ... load configs ...
        
        if validate_config:
            validator = ConfigValidator(self._check_registry, self._stage_registry)
            errors = validator.validate_all(self._pipelines_config, self._checks_config)
            
            # Separate errors from warnings
            critical_errors = [e for e in errors if e.severity == "error"]
            warnings = [e for e in errors if e.severity == "warning"]
            
            # Log warnings
            for warning in warnings:
                logger.warning(f"Config warning at {warning.path}: {warning.message}")
            
            # Raise on critical errors
            if critical_errors:
                error_messages = "\n".join(
                    f"  - {e.path}: {e.message}" for e in critical_errors
                )
                raise ConfigurationError(
                    f"Invalid configuration:\n{error_messages}"
                )
```

#### Implementation Approach

The `ConfigValidator` uses several techniques to validate configuration:

**1. Valid Dtype Enumeration**

```python
VALID_POLARS_DTYPES = {
    "Int8", "Int16", "Int32", "Int64",
    "UInt8", "UInt16", "UInt32", "UInt64",
    "Float32", "Float64",
    "Boolean", "Utf8", "String",
    "Date", "Datetime", "Time", "Duration",
    "Categorical", "Null", "Object",
}

def _validate_dtype(self, dtype: str, path: str) -> Optional[ConfigValidationError]:
    if dtype not in VALID_POLARS_DTYPES:
        suggestions = difflib.get_close_matches(dtype, VALID_POLARS_DTYPES, n=1)
        suggestion_text = f" - did you mean '{suggestions[0]}'?" if suggestions else ""
        return ConfigValidationError(
            path=path,
            message=f"Unknown dtype '{dtype}'{suggestion_text}",
            severity="error"
        )
    return None
```

**2. Pandera Built-in Check Validation**

```python
VALID_PANDERA_CHECKS = {
    "greater_than", "greater_than_or_equal_to", 
    "less_than", "less_than_or_equal_to",
    "in_range", "equal_to", "not_equal_to",
    "isin", "notin",
    "str_matches", "str_contains", "str_startswith", "str_endswith", "str_length",
}

def _validate_builtin_check(self, check_name: str, path: str) -> Optional[ConfigValidationError]:
    if check_name not in VALID_PANDERA_CHECKS:
        return ConfigValidationError(
            path=path,
            message=f"Unknown Pandera check '{check_name}'",
            severity="error"
        )
    return None
```

**3. Parameter Validation via Introspection**

Custom check parameter validation uses Python's `inspect` module to examine the check class `__init__` signature:

```python
import inspect
from typing import get_type_hints

def _validate_check_params(
    self, 
    check_id: str, 
    params: Dict[str, Any], 
    path: str
) -> List[ConfigValidationError]:
    """Validate params match the check class __init__ signature."""
    errors = []
    
    check_class = self._check_registry.get(check_id)
    if check_class is None:
        return errors  # Already reported as missing check
    
    sig = inspect.signature(check_class.__init__)
    
    # Check if the class accepts **kwargs
    accepts_kwargs = any(
        param.kind == inspect.Parameter.VAR_KEYWORD
        for param in sig.parameters.values()
    )
    
    required_params = {
        name for name, param in sig.parameters.items()
        if param.default is inspect.Parameter.empty
        and name not in ("self",)
        and param.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
    }
    optional_params = {
        name for name, param in sig.parameters.items()
        if param.default is not inspect.Parameter.empty
        and param.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
    }
    all_params = required_params | optional_params
    
    # Check for missing required params
    provided = set(params.keys())
    missing = required_params - provided
    for param_name in missing:
        errors.append(ConfigValidationError(
            path=f"{path}.params",
            message=f"Missing required parameter '{param_name}' for check '{check_id}'",
            severity="error"
        ))
    
    # Check for unknown params (skip if check accepts **kwargs)
    if not accepts_kwargs:
        unknown = provided - all_params
        for param_name in unknown:
            errors.append(ConfigValidationError(
                path=f"{path}.params.{param_name}",
                message=f"Unknown parameter '{param_name}' for check '{check_id}'",
                severity="warning"
            ))
    
    return errors
```

**4. JSON Schema Validation (Optional)**

For static validation and IDE support, JSON Schema files are provided:

```
frameworks/data_validation/
├── schemas/
│   ├── validation_pipelines.schema.json    # Schema for pipeline config
│   └── custom_checks.schema.json           # Schema for check config
```

Example schema excerpt for pipeline configuration:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "Validation Pipelines Configuration",
  "type": "object",
  "properties": {
    "validation_pipelines": {
      "type": "object",
      "additionalProperties": {
        "$ref": "#/$defs/Pipeline"
      }
    }
  },
  "$defs": {
    "Pipeline": {
      "type": "object",
      "required": ["stages"],
      "properties": {
        "description": { "type": "string" },
        "on_failure": { "enum": ["fail_fast", "collect_all"], "default": "collect_all" },
        "stages": {
          "type": "array",
          "items": { "$ref": "#/$defs/Stage" },
          "minItems": 1
        }
      }
    },
    "Stage": {
      "type": "object",
      "required": ["name", "type", "config"],
      "properties": {
        "name": { "type": "string" },
        "type": { "enum": ["schema_validation", "custom_rules", "cross_field_validation", "referential_validation"] },
        "config": { "type": "object" }
      }
    },
    "Dtype": {
      "enum": ["Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64", 
               "Float32", "Float64", "Boolean", "Utf8", "String", "Date", "Datetime", 
               "Time", "Duration", "Categorical", "Null", "Object"]
    }
  }
}
```

The JSON Schema files enable:
- IDE autocomplete and validation in VS Code, PyCharm, etc.
- Pre-commit hooks for config file validation
- CI/CD pipeline validation before deployment
```

---

## Validation Stage Types

The framework supports four built-in stage types. Each stage type has a specific purpose and configuration schema.

### Stage Type Registry

```python
STAGE_TYPES = {
    "schema_validation": SchemaValidationStage,
    "custom_rules": CustomRulesStage,
    "cross_field_validation": CrossFieldValidationStage,
    "referential_validation": ReferentialValidationStage,
}
```

---

### Stage Type 1: `schema_validation`

**Purpose**: Validates DataFrame structure and column data types using Pandera's schema validation for Polars.

**Pandera Integration**: This stage directly uses `pandera.polars.DataFrameSchema` and `pandera.polars.Column` to build and validate schemas.

#### Configuration Schema

```json
{
  "name": "string (required)",
  "type": "schema_validation",
  "config": {
    "coerce": "boolean (default: false)",
    "strict": "boolean (default: false)",
    "drop_invalid_rows": "boolean (default: false) - see Drop Invalid Rows Behavior below",
    "treat_dropped_as_failure": "boolean (default: false) - if true, stage is_valid=false even when rows are dropped",
    "columns": {
      "<column_name>": {
        "dtype": "string (Polars dtype: Int64, Float64, Utf8, Boolean, Date, Datetime, etc.)",
        "nullable": "boolean (default: true)",
        "unique": "boolean (default: false)",
        "required": "boolean (default: true)",
        "checks": [
          {
            "builtin": "string (Pandera check name)",
            "<param>": "value"
          }
        ]
      }
    },
    "dataframe_checks": [
      {
        "builtin": "string (Pandera dataframe check name)",
        "<param>": "value"
      }
    ]
  }
}
```

#### Supported Pandera Built-in Checks

| Check Name | Parameters | Description |
|------------|------------|-------------|
| `greater_than` | `value` | Value must be > value |
| `greater_than_or_equal_to` | `value` | Value must be >= value |
| `less_than` | `value` | Value must be < value |
| `less_than_or_equal_to` | `value` | Value must be <= value |
| `in_range` | `min_value`, `max_value`, `include_min`, `include_max` | Value must be within range |
| `equal_to` | `value` | Value must equal value |
| `not_equal_to` | `value` | Value must not equal value |
| `isin` | `allowed_values` (list) | Value must be in list |
| `notin` | `forbidden_values` (list) | Value must not be in list |
| `str_matches` | `pattern` | String must match regex pattern |
| `str_contains` | `pattern` | String must contain pattern |
| `str_startswith` | `string` | String must start with value |
| `str_endswith` | `string` | String must end with value |
| `str_length` | `min_value`, `max_value` | String length must be in range |

#### Implementation

> **Note**: The authoritative implementation with proper timing and data propagation is in Section 4 "Data Propagation Model". The implementation below shows the class structure and schema building logic.

```python
class SchemaValidationStage(BaseValidationStage):
    """Validates DataFrame schema using Pandera for Polars."""
    
    stage_type = "schema_validation"
    
    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        self._config = config
        self._schema = self._build_pandera_schema(config)
    
    def _build_pandera_schema(self, config: Dict[str, Any]) -> pa.polars.DataFrameSchema:
        """Build a Pandera Polars schema from configuration."""
        import pandera.polars as pa
        
        columns = {}
        for col_name, col_config in config.get("columns", {}).items():
            checks = self._build_checks(col_config.get("checks", []))
            columns[col_name] = pa.Column(
                dtype=self._map_dtype(col_config.get("dtype")),
                nullable=col_config.get("nullable", True),
                unique=col_config.get("unique", False),
                checks=checks,
            )
        
        df_checks = self._build_checks(config.get("dataframe_checks", []))
        
        return pa.DataFrameSchema(
            columns=columns,
            checks=df_checks,
            coerce=config.get("coerce", False),
            strict=config.get("strict", False),
        )
    
    def _build_checks(self, check_configs: List[Dict]) -> List[pa.Check]:
        """Build Pandera Check objects from configuration."""
        checks = []
        for check_config in check_configs:
            if "builtin" in check_config:
                check_name = check_config["builtin"]
                params = {k: v for k, v in check_config.items() if k != "builtin"}
                check_method = getattr(pa.Check, check_name)
                checks.append(check_method(**params))
        return checks
    
    # Note: The execute() method is defined in Section 4 "Data Propagation Model"
    # with proper timing, output_data handling, and drop_invalid_rows support.
```

#### Example Configuration

```json
{
  "name": "validate_customer_schema",
  "type": "schema_validation",
  "config": {
    "coerce": true,
    "strict": false,
    "columns": {
      "customer_id": {
        "dtype": "Int64",
        "nullable": false,
        "unique": true,
        "checks": [
          {"builtin": "greater_than", "value": 0}
        ]
      },
      "email": {
        "dtype": "Utf8",
        "nullable": false,
        "checks": [
          {"builtin": "str_matches", "pattern": "^[\\w.-]+@[\\w.-]+\\.\\w+$"}
        ]
      },
      "age": {
        "dtype": "Int64",
        "nullable": true,
        "checks": [
          {"builtin": "in_range", "min_value": 0, "max_value": 150}
        ]
      },
      "registration_date": {
        "dtype": "Date",
        "nullable": false
      },
      "status": {
        "dtype": "Utf8",
        "nullable": false,
        "checks": [
          {"builtin": "isin", "allowed_values": ["active", "inactive", "pending"]}
        ]
      }
    }
  }
}
```

---

### Stage Type 2: `custom_rules`

**Purpose**: Executes custom validation checks that are not supported by Pandera's built-in checks. Custom checks are defined separately and referenced by ID.

#### Configuration Schema

```json
{
  "name": "string (required)",
  "type": "custom_rules",
  "config": {
    "fail_fast": "boolean (default: false)",
    "rules": [
      {
        "check_id": "string (required - references registered custom check)",
        "column": "string (optional - for column-level checks)",
        "columns": ["string"] ,
        "params": {
          "<param_name>": "value"
        },
        "error_message": "string (optional - custom error message)",
        "raise_warning": "boolean (default: false)"
      }
    ]
  }
}
```

#### Implementation

```python
class CustomRulesStage(BaseValidationStage):
    """Executes custom validation rules."""
    
    stage_type = "custom_rules"
    
    def __init__(self, name: str, config: Dict[str, Any], check_registry: CheckRegistry):
        super().__init__(name, config)
        self._check_registry = check_registry
        self._rules = config.get("rules", [])
        self._fail_fast = config.get("fail_fast", False)
    
    def execute(self, context: ValidationContext) -> StageResult:
        """Execute all custom rules."""
        errors = []
        warnings = []
        
        for rule in self._rules:
            check_id = rule["check_id"]
            check_class = self._check_registry.get(check_id)
            
            if check_class is None:
                raise ValueError(f"Custom check '{check_id}' not found in registry")
            
            check_instance = check_class(**rule.get("params", {}))
            
            # Determine if column-level or dataframe-level check
            if "column" in rule:
                result = check_instance.validate_column(
                    context.data, 
                    rule["column"]
                )
            elif "columns" in rule:
                result = check_instance.validate_columns(
                    context.data,
                    rule["columns"]
                )
            else:
                result = check_instance.validate_dataframe(context.data)
            
            if not result.is_valid:
                error = ValidationError(
                    check_name=check_id,
                    column=rule.get("column"),
                    error_message=rule.get("error_message", result.message),
                    failure_cases=result.failure_cases,
                    row_indices=result.row_indices,
                )
                if rule.get("raise_warning", False):
                    warnings.append(error)
                else:
                    errors.append(error)
                    if self._fail_fast:
                        break
        
        return StageResult(
            stage_name=self.name,
            stage_type=self.stage_type,
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            execution_time_ms=0,
            rows_validated=len(context.data),
            rows_failed=len(set(idx for e in errors if e.row_indices for idx in e.row_indices)),
        )
```

#### Example Configuration

```json
{
  "name": "validate_business_rules",
  "type": "custom_rules",
  "config": {
    "fail_fast": false,
    "rules": [
      {
        "check_id": "is_valid_email_domain",
        "column": "email",
        "params": {
          "allowed_domains": ["gmail.com", "yahoo.com", "company.com"]
        },
        "error_message": "Email domain is not in the allowed list"
      },
      {
        "check_id": "is_working_age",
        "column": "age",
        "params": {
          "min_age": 18,
          "max_age": 65
        }
      },
      {
        "check_id": "is_future_date",
        "column": "registration_date",
        "params": {
          "allow_today": true
        },
        "raise_warning": true
      }
    ]
  }
}
```

---

### Stage Type 3: `cross_field_validation`

**Purpose**: Validates relationships and constraints across multiple columns within the same row.

#### Configuration Schema

```json
{
  "name": "string (required)",
  "type": "cross_field_validation",
  "config": {
    "fail_fast": "boolean (default: false)",
    "rules": [
      {
        "check_id": "string (required - references registered custom check)",
        "columns": ["string (required - list of columns involved)"],
        "params": {
          "<param_name>": "value"
        },
        "error_message": "string (optional)"
      }
    ]
  }
}
```

#### Implementation

```python
class CrossFieldValidationStage(BaseValidationStage):
    """Validates cross-field constraints."""
    
    stage_type = "cross_field_validation"
    
    def __init__(self, name: str, config: Dict[str, Any], check_registry: CheckRegistry):
        super().__init__(name, config)
        self._check_registry = check_registry
        self._rules = config.get("rules", [])
        self._fail_fast = config.get("fail_fast", False)
    
    def execute(self, context: ValidationContext) -> StageResult:
        """Execute cross-field validation rules."""
        errors = []
        
        for rule in self._rules:
            check_id = rule["check_id"]
            columns = rule["columns"]
            check_class = self._check_registry.get(check_id)
            
            if check_class is None:
                raise ValueError(f"Custom check '{check_id}' not found in registry")
            
            check_instance = check_class(**rule.get("params", {}))
            result = check_instance.validate_columns(context.data, columns)
            
            if not result.is_valid:
                errors.append(ValidationError(
                    check_name=check_id,
                    column=", ".join(columns),
                    error_message=rule.get("error_message", result.message),
                    failure_cases=result.failure_cases,
                    row_indices=result.row_indices,
                ))
                if self._fail_fast:
                    break
        
        return StageResult(
            stage_name=self.name,
            stage_type=self.stage_type,
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=[],
            execution_time_ms=0,
            rows_validated=len(context.data),
            rows_failed=len(set(idx for e in errors if e.row_indices for idx in e.row_indices)),
        )
```

#### Example Configuration

```json
{
  "name": "validate_cross_field_rules",
  "type": "cross_field_validation",
  "config": {
    "fail_fast": false,
    "rules": [
      {
        "check_id": "date_order",
        "columns": ["start_date", "end_date"],
        "params": {
          "allow_equal": true
        },
        "error_message": "start_date must be before or equal to end_date"
      },
      {
        "check_id": "conditional_required",
        "columns": ["status", "cancellation_reason"],
        "params": {
          "condition_column": "status",
          "condition_value": "cancelled",
          "required_column": "cancellation_reason"
        },
        "error_message": "cancellation_reason is required when status is 'cancelled'"
      },
      {
        "check_id": "sum_equals",
        "columns": ["quantity_a", "quantity_b", "total_quantity"],
        "params": {
          "sum_columns": ["quantity_a", "quantity_b"],
          "total_column": "total_quantity",
          "tolerance": 0.01
        }
      }
    ]
  }
}
```

---

### Stage Type 4: `referential_validation`

**Purpose**: Validates referential integrity against external reference datasets (e.g., foreign key lookups, master data validation).

#### Configuration Schema

```json
{
  "name": "string (required)",
  "type": "referential_validation",
  "config": {
    "fail_fast": "boolean (default: false)",
    "rules": [
      {
        "check_id": "string (required - typically 'exists_in' or custom)",
        "column": "string (required - column to validate)",
        "reference_dataset": "string (required - key in context.reference_data)",
        "reference_column": "string (required - column in reference dataset)",
        "params": {
          "<param_name>": "value"
        },
        "error_message": "string (optional)"
      }
    ]
  }
}
```

#### Implementation

```python
class ReferentialValidationStage(BaseValidationStage):
    """Validates referential integrity against reference datasets."""
    
    stage_type = "referential_validation"
    
    def __init__(self, name: str, config: Dict[str, Any], check_registry: CheckRegistry):
        super().__init__(name, config)
        self._check_registry = check_registry
        self._rules = config.get("rules", [])
        self._fail_fast = config.get("fail_fast", False)
    
    def execute(self, context: ValidationContext) -> StageResult:
        """Execute referential validation rules."""
        errors = []
        
        for rule in self._rules:
            ref_dataset_key = rule["reference_dataset"]
            
            if ref_dataset_key not in context.reference_data:
                raise ValueError(
                    f"Reference dataset '{ref_dataset_key}' not found in context"
                )
            
            ref_df = context.reference_data[ref_dataset_key]
            check_id = rule["check_id"]
            check_class = self._check_registry.get(check_id)
            
            if check_class is None:
                raise ValueError(f"Custom check '{check_id}' not found in registry")
            
            check_instance = check_class(
                reference_data=ref_df,
                reference_column=rule["reference_column"],
                **rule.get("params", {})
            )
            
            result = check_instance.validate_column(context.data, rule["column"])
            
            if not result.is_valid:
                errors.append(ValidationError(
                    check_name=check_id,
                    column=rule["column"],
                    error_message=rule.get("error_message", result.message),
                    failure_cases=result.failure_cases,
                    row_indices=result.row_indices,
                ))
                if self._fail_fast:
                    break
        
        return StageResult(
            stage_name=self.name,
            stage_type=self.stage_type,
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=[],
            execution_time_ms=0,
            rows_validated=len(context.data),
            rows_failed=len(set(idx for e in errors if e.row_indices for idx in e.row_indices)),
        )
```

#### Example Configuration

```json
{
  "name": "validate_references",
  "type": "referential_validation",
  "config": {
    "fail_fast": false,
    "rules": [
      {
        "check_id": "exists_in",
        "column": "country_code",
        "reference_dataset": "countries",
        "reference_column": "code",
        "error_message": "Invalid country code - not found in reference data"
      },
      {
        "check_id": "exists_in",
        "column": "product_id",
        "reference_dataset": "products",
        "reference_column": "id",
        "params": {
          "case_sensitive": true
        },
        "error_message": "Product ID does not exist in product catalog"
      },
      {
        "check_id": "exists_in_with_condition",
        "column": "category_id",
        "reference_dataset": "categories",
        "reference_column": "id",
        "params": {
          "filter_column": "is_active",
          "filter_value": true
        },
        "error_message": "Category must exist and be active"
      }
    ]
  }
}
```

---

## Core Contracts (ABCs)

This section defines the abstract base classes that form the contract layer of the framework. These ABCs ensure consistency across implementations and enable extensibility.

### ValidationStage ABC

The `ValidationStage` ABC defines the contract for all validation stage implementations:

```python
# frameworks/data_validation/contract/validation_stage.py

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from ..engine.validation_context import ValidationContext
from ..engine.validation_result import StageResult


class ValidationStage(ABC):
    """
    Abstract base class for validation stages.
    
    Each stage type (schema_validation, custom_rules, etc.) must implement
    this contract to be usable in validation pipelines.
    """
    
    @property
    @abstractmethod
    def stage_type(self) -> str:
        """
        Unique identifier for this stage type.
        
        This must match the 'type' field in pipeline configuration.
        Examples: 'schema_validation', 'custom_rules', 'cross_field_validation'
        """
        pass
    
    @property
    @abstractmethod
    def name(self) -> str:
        """
        Instance name for this stage.
        
        This is the name specified in the pipeline configuration.
        """
        pass
    
    @abstractmethod
    def execute(self, context: ValidationContext) -> StageResult:
        """
        Execute this validation stage.
        
        Args:
            context: The validation context containing data and metadata
            
        Returns:
            StageResult containing validation outcome for this stage
        """
        pass
    
    def validate_config(self, config: Dict[str, Any]) -> Optional[str]:
        """
        Validate the stage configuration before execution.
        
        Override this method to add custom configuration validation.
        
        Args:
            config: The stage configuration dictionary
            
        Returns:
            None if valid, error message string if invalid
        """
        return None
```

### Validator ABC

The `Validator` ABC defines the contract for the main validation engine:

```python
# frameworks/data_validation/contract/validator.py

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import polars as pl

from ..engine.validation_result import ValidationResult


class Validator(ABC):
    """
    Abstract base class for validation engines.
    
    Defines the core interface for validating DataFrames through pipelines.
    """
    
    @abstractmethod
    def validate(
        self,
        pipeline_id: str,
        data: pl.DataFrame,
        context: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        """
        Execute a validation pipeline on the given data.
        
        Args:
            pipeline_id: ID of the validation pipeline to execute
            data: Polars DataFrame to validate
            context: Optional context data (e.g., reference datasets)
            
        Returns:
            ValidationResult containing all stage results
        """
        pass
    
    @abstractmethod
    def register_check(self, check_id: str, check_class: type) -> None:
        """
        Register a custom check class with the validator.
        
        Args:
            check_id: Unique identifier for the check
            check_class: The check class (must implement CustomCheck)
        """
        pass
    
    @abstractmethod
    def register_stage(self, stage_type: str, stage_class: type) -> None:
        """
        Register a custom stage type with the validator.
        
        Args:
            stage_type: Unique identifier for the stage type
            stage_class: The stage class (must implement ValidationStage)
        """
        pass
    
    @abstractmethod
    def list_pipelines(self) -> List[str]:
        """
        List all registered validation pipeline IDs.
        
        Returns:
            List of pipeline ID strings
        """
        pass
    
    @abstractmethod
    def list_checks(self) -> List[str]:
        """
        List all registered custom check IDs.
        
        Returns:
            List of check ID strings
        """
        pass
```

### CustomCheck ABC

Custom checks are defined in a separate configuration file and must be registered before use. This separation allows custom checks to be reused across multiple validation pipelines.

```python
# frameworks/data_validation/contract/check.py

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import polars as pl


@dataclass
class CheckResult:
    """Result of a custom check execution."""
    
    is_valid: bool
    message: str
    failure_cases: Optional[List[Any]] = None
    row_indices: Optional[List[int]] = None


class CustomCheck(ABC):
    """
    Abstract base class for custom validation checks.
    
    Note: The check's identity is determined by its registry key, not by a class
    attribute. This allows the same check class to be registered under multiple
    IDs with different default parameters if needed.
    """
    
    @property
    def description(self) -> str:
        """Human-readable description of what this check validates."""
        return ""
    
    def validate_column(
        self, 
        df: pl.DataFrame, 
        column: str
    ) -> CheckResult:
        """
        Validate a single column.
        
        Override this method for column-level checks.
        """
        raise NotImplementedError("Column validation not implemented for this check")
    
    def validate_columns(
        self, 
        df: pl.DataFrame, 
        columns: List[str]
    ) -> CheckResult:
        """
        Validate multiple columns together.
        
        Override this method for cross-field checks.
        """
        raise NotImplementedError("Multi-column validation not implemented for this check")
    
    def validate_dataframe(
        self, 
        df: pl.DataFrame
    ) -> CheckResult:
        """
        Validate the entire dataframe.
        
        Override this method for dataframe-level checks.
        """
        raise NotImplementedError("DataFrame validation not implemented for this check")
```

---

## Custom Check Definition

Custom checks are classes that implement the `CustomCheck` ABC (defined in Core Contracts above) and are registered with the validation engine. The registration mapping is defined in a configuration file that maps check IDs to their implementation classes.

### Configuration File Schema

```json
{
  "custom_checks": {
    "<check_id>": {
      "module": "string (required - Python module path)",
      "class": "string (required - class name)",
      "description": "string (optional - human-readable description)",
      "default_params": {
        "<param_name>": "value (optional - default parameter values)"
      }
    }
  }
}
```

### Parameter Merge Rules

When a rule references a custom check, parameters are resolved by merging `default_params` from the check configuration with `params` from the rule configuration.

**Merge Order**: Rule-level params override check-level defaults.

```python
# In CheckRegistry or stage execution:
def get_merged_params(check_config: Dict, rule_params: Dict) -> Dict:
    """Merge default params with rule-level params."""
    default_params = check_config.get("default_params", {})
    return {**default_params, **rule_params}  # rule params win
```

**Example**:

```json
// custom_checks.json
{
  "custom_checks": {
    "is_working_age": {
      "module": "...",
      "class": "WorkingAgeCheck",
      "default_params": {
        "min_age": 18,
        "max_age": 65
      }
    }
  }
}

// validation_pipelines.json - rule overrides max_age
{
  "check_id": "is_working_age",
  "column": "age",
  "params": {
    "max_age": 100  // Override: final params = {min_age: 18, max_age: 100}
  }
}
```

### Duplicate Check ID Handling

Check IDs must be unique within a configuration file. If duplicates are detected:

1. **At load time**: The `ConfigValidator` reports an error for duplicate check IDs.
2. **Programmatic registration**: Later `register_check()` calls with the same ID raise a `ValueError` unless `overwrite=True` is specified.

```python
class CheckRegistry:
    def register(self, check_id: str, check_class: type, overwrite: bool = False) -> None:
        if check_id in self._checks and not overwrite:
            raise ValueError(
                f"Check '{check_id}' is already registered. "
                f"Use overwrite=True to replace it."
            )
        self._checks[check_id] = check_class
```

#### Example: `custom_checks.json`

```json
{
  "custom_checks": {
    "is_valid_email_domain": {
      "module": "frameworks.data_validation.checks.builtin.string_checks",
      "class": "ValidEmailDomainCheck",
      "description": "Validates that email addresses have an allowed domain",
      "default_params": {
        "allowed_domains": []
      }
    },
    "is_working_age": {
      "module": "frameworks.data_validation.checks.builtin.numeric_checks",
      "class": "WorkingAgeCheck",
      "description": "Validates that age is within working age range",
      "default_params": {
        "min_age": 18,
        "max_age": 65
      }
    },
    "is_future_date": {
      "module": "frameworks.data_validation.checks.builtin.date_checks",
      "class": "FutureDateCheck",
      "description": "Validates that a date is not in the future",
      "default_params": {
        "allow_today": true
      }
    },
    "date_order": {
      "module": "frameworks.data_validation.checks.builtin.date_checks",
      "class": "DateOrderCheck",
      "description": "Validates that one date is before another",
      "default_params": {
        "allow_equal": false
      }
    },
    "conditional_required": {
      "module": "frameworks.data_validation.checks.builtin.business_checks",
      "class": "ConditionalRequiredCheck",
      "description": "Validates that a field is required based on another field's value"
    },
    "sum_equals": {
      "module": "frameworks.data_validation.checks.builtin.numeric_checks",
      "class": "SumEqualsCheck",
      "description": "Validates that sum of columns equals a total column",
      "default_params": {
        "tolerance": 0.0
      }
    },
    "exists_in": {
      "module": "frameworks.data_validation.checks.builtin.referential_checks",
      "class": "ExistsInCheck",
      "description": "Validates that values exist in a reference dataset",
      "default_params": {
        "case_sensitive": true
      }
    },
    "exists_in_with_condition": {
      "module": "frameworks.data_validation.checks.builtin.referential_checks",
      "class": "ExistsInWithConditionCheck",
      "description": "Validates existence with additional filter condition"
    },
    "unique_combination": {
      "module": "frameworks.data_validation.checks.builtin.business_checks",
      "class": "UniqueCombinationCheck",
      "description": "Validates that combination of columns is unique"
    }
  }
}
```

### Example Custom Check Implementation

```python
# frameworks/data_validation/checks/builtin/string_checks.py

import polars as pl
from typing import List, Optional
from ..base_check import CustomCheck, CheckResult


class ValidEmailDomainCheck(CustomCheck):
    """Validates that email addresses have an allowed domain."""
    
    description = "Validates that email addresses have an allowed domain"
    
    def __init__(self, allowed_domains: List[str]):
        self.allowed_domains = [d.lower() for d in allowed_domains]
    
    def validate_column(self, df: pl.DataFrame, column: str) -> CheckResult:
        # Extract domain from email
        domains = df.select(
            pl.col(column)
            .str.split("@")
            .list.last()
            .str.to_lowercase()
            .alias("domain")
        )
        
        # Check if domain is in allowed list
        invalid_mask = ~domains["domain"].is_in(self.allowed_domains)
        invalid_indices = df.with_row_index().filter(invalid_mask)["index"].to_list()
        
        if len(invalid_indices) > 0:
            invalid_values = df[column].gather(invalid_indices[:10]).to_list()
            return CheckResult(
                is_valid=False,
                message=f"Found {len(invalid_indices)} email(s) with invalid domains",
                failure_cases=invalid_values,
                row_indices=invalid_indices,
            )
        
        return CheckResult(is_valid=True, message="All email domains are valid")


class DateOrderCheck(CustomCheck):
    """Validates that one date column is before another."""
    
    description = "Validates that first date is before second date"
    
    def __init__(self, allow_equal: bool = False):
        self.allow_equal = allow_equal
    
    def validate_columns(self, df: pl.DataFrame, columns: List[str]) -> CheckResult:
        if len(columns) != 2:
            raise ValueError("date_order check requires exactly 2 columns")
        
        start_col, end_col = columns
        
        if self.allow_equal:
            invalid_mask = pl.col(start_col) > pl.col(end_col)
        else:
            invalid_mask = pl.col(start_col) >= pl.col(end_col)
        
        invalid_df = df.with_row_index().filter(invalid_mask)
        invalid_indices = invalid_df["index"].to_list()
        
        if len(invalid_indices) > 0:
            return CheckResult(
                is_valid=False,
                message=f"Found {len(invalid_indices)} rows where {start_col} is not before {end_col}",
                failure_cases=None,
                row_indices=invalid_indices,
            )
        
        return CheckResult(is_valid=True, message="Date order is valid")
```

---

## Validation Pipeline Configuration

Validation pipelines are defined in a JSON/YAML configuration file that combines multiple stages.

### Schema

```json
{
  "validation_pipelines": {
    "<pipeline_id>": {
      "description": "string (optional)",
      "on_failure": "string (fail_fast | collect_all, default: collect_all)",
      "stages": [
        {
          "name": "string (required)",
          "type": "string (required - stage type)",
          "config": {}
        }
      ]
    }
  }
}
```

### Example: `validation_pipelines.json`

```json
{
  "validation_pipelines": {
    "customer_data_validation": {
      "description": "Complete validation pipeline for customer data",
      "on_failure": "collect_all",
      "stages": [
        {
          "name": "schema_validation",
          "type": "schema_validation",
          "config": {
            "coerce": true,
            "strict": false,
            "columns": {
              "customer_id": {
                "dtype": "Int64",
                "nullable": false,
                "unique": true,
                "checks": [
                  {"builtin": "greater_than", "value": 0}
                ]
              },
              "email": {
                "dtype": "Utf8",
                "nullable": false,
                "checks": [
                  {"builtin": "str_matches", "pattern": "^[\\w.-]+@[\\w.-]+\\.\\w+$"}
                ]
              },
              "age": {
                "dtype": "Int64",
                "nullable": true,
                "checks": [
                  {"builtin": "in_range", "min_value": 0, "max_value": 150}
                ]
              },
              "country_code": {
                "dtype": "Utf8",
                "nullable": false,
                "checks": [
                  {"builtin": "str_length", "min_value": 2, "max_value": 3}
                ]
              },
              "registration_date": {
                "dtype": "Date",
                "nullable": false
              },
              "status": {
                "dtype": "Utf8",
                "nullable": false,
                "checks": [
                  {"builtin": "isin", "allowed_values": ["active", "inactive", "pending", "cancelled"]}
                ]
              },
              "cancellation_reason": {
                "dtype": "Utf8",
                "nullable": true
              }
            }
          }
        },
        {
          "name": "business_rules",
          "type": "custom_rules",
          "config": {
            "fail_fast": false,
            "rules": [
              {
                "check_id": "is_valid_email_domain",
                "column": "email",
                "params": {
                  "allowed_domains": ["gmail.com", "yahoo.com", "hotmail.com", "company.com"]
                }
              },
              {
                "check_id": "is_working_age",
                "column": "age",
                "params": {
                  "min_age": 18,
                  "max_age": 100
                }
              },
              {
                "check_id": "is_future_date",
                "column": "registration_date",
                "params": {
                  "allow_today": true
                },
                "raise_warning": true
              }
            ]
          }
        },
        {
          "name": "cross_field_rules",
          "type": "cross_field_validation",
          "config": {
            "rules": [
              {
                "check_id": "conditional_required",
                "columns": ["status", "cancellation_reason"],
                "params": {
                  "condition_column": "status",
                  "condition_value": "cancelled",
                  "required_column": "cancellation_reason"
                },
                "error_message": "cancellation_reason is required when status is 'cancelled'"
              }
            ]
          }
        },
        {
          "name": "referential_integrity",
          "type": "referential_validation",
          "config": {
            "rules": [
              {
                "check_id": "exists_in",
                "column": "country_code",
                "reference_dataset": "countries",
                "reference_column": "code",
                "error_message": "Invalid country code"
              }
            ]
          }
        }
      ]
    },
    "order_data_validation": {
      "description": "Validation pipeline for order data",
      "on_failure": "fail_fast",
      "stages": [
        {
          "name": "schema_check",
          "type": "schema_validation",
          "config": {
            "columns": {
              "order_id": {
                "dtype": "Utf8",
                "nullable": false,
                "unique": true
              },
              "customer_id": {
                "dtype": "Int64",
                "nullable": false
              },
              "order_date": {
                "dtype": "Date",
                "nullable": false
              },
              "ship_date": {
                "dtype": "Date",
                "nullable": true
              },
              "quantity_a": {
                "dtype": "Float64",
                "nullable": false,
                "checks": [
                  {"builtin": "greater_than_or_equal_to", "value": 0}
                ]
              },
              "quantity_b": {
                "dtype": "Float64",
                "nullable": false,
                "checks": [
                  {"builtin": "greater_than_or_equal_to", "value": 0}
                ]
              },
              "total_quantity": {
                "dtype": "Float64",
                "nullable": false
              }
            }
          }
        },
        {
          "name": "date_consistency",
          "type": "cross_field_validation",
          "config": {
            "rules": [
              {
                "check_id": "date_order",
                "columns": ["order_date", "ship_date"],
                "params": {
                  "allow_equal": true
                },
                "error_message": "ship_date must be on or after order_date"
              },
              {
                "check_id": "sum_equals",
                "columns": ["quantity_a", "quantity_b", "total_quantity"],
                "params": {
                  "sum_columns": ["quantity_a", "quantity_b"],
                  "total_column": "total_quantity",
                  "tolerance": 0.01
                },
                "error_message": "total_quantity must equal sum of quantity_a and quantity_b"
              }
            ]
          }
        },
        {
          "name": "customer_exists",
          "type": "referential_validation",
          "config": {
            "rules": [
              {
                "check_id": "exists_in",
                "column": "customer_id",
                "reference_dataset": "customers",
                "reference_column": "customer_id",
                "error_message": "Customer does not exist"
              }
            ]
          }
        }
      ]
    }
  }
}
```

---

## Service Pipeline Integration

The Data Validation Framework integrates with the Service Pipeline Framework through an adapter component.

### Adapter Component

```python
# frameworks/data_validation/adapters/pipeline_adapter.py

from typing import Dict, Any, Optional
import polars as pl

from frameworks.service_pipeline.contract.component import Component
from frameworks.service_pipeline.implementation.base_component import BaseComponent
from ..engine.validation_engine import ValidationEngine
from ..engine.validation_result import ValidationResult


class DataValidationComponent(BaseComponent):
    """
    Service Pipeline adapter for the Data Validation Framework.
    
    Allows validation pipelines to be used as steps in service pipelines.
    """
    
    def __init__(self, config: Dict[str, Any] | None = None):
        """
        Initialize the validation component.
        
        Config options:
            pipeline_config_path: Path to validation pipelines config file
            check_config_path: Path to custom checks config file
            pipeline_id: ID of the validation pipeline to execute
            input_key: Key in context containing the DataFrame to validate (default: "data")
            output_key: Key for validated data in output (default: "validated_data")
            reference_data_keys: Dict mapping reference dataset names to context keys
            on_validation_failure: Action on failure: "error" | "continue" (default: "error")
            include_result_details: Include detailed results in context (default: true)
        """
        super().__init__(config)
        
        self._pipeline_config_path = self.config.get("pipeline_config_path")
        self._check_config_path = self.config.get("check_config_path")
        self._pipeline_id = self.config.get("pipeline_id")
        self._input_key = self.config.get("input_key", "data")
        self._output_key = self.config.get("output_key", "validated_data")
        self._reference_data_keys = self.config.get("reference_data_keys", {})
        self._on_failure = self.config.get("on_validation_failure", "error")
        self._include_details = self.config.get("include_result_details", True)
        
        # Initialize validation engine
        self._engine = ValidationEngine(
            pipeline_config_path=self._pipeline_config_path,
            check_config_path=self._check_config_path,
        )
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute validation pipeline on data from context.
        
        Args:
            context: Service pipeline context containing input data
            
        Returns:
            Updated context with validation results
        """
        super().execute(context)
        
        # Get input data
        input_data = context.get(self._input_key)
        if input_data is None:
            raise ValueError(f"Input key '{self._input_key}' not found in context")
        
        # Convert to Polars DataFrame if necessary
        if not isinstance(input_data, pl.DataFrame):
            if hasattr(input_data, 'to_polars'):
                input_data = input_data.to_polars()
            elif isinstance(input_data, dict):
                input_data = pl.DataFrame(input_data)
            elif isinstance(input_data, list):
                input_data = pl.DataFrame(input_data)
            else:
                raise TypeError(
                    f"Cannot convert input of type {type(input_data)} to Polars DataFrame"
                )
        
        # Gather reference data from context
        reference_data = {}
        for ref_name, context_key in self._reference_data_keys.items():
            ref_df = context.get(context_key)
            if ref_df is not None:
                if not isinstance(ref_df, pl.DataFrame):
                    ref_df = pl.DataFrame(ref_df)
                reference_data[ref_name] = ref_df
        
        # Execute validation
        self.log_info(f"Executing validation pipeline '{self._pipeline_id}'")
        result = self._engine.validate(
            pipeline_id=self._pipeline_id,
            data=input_data,
            context={"reference_data": reference_data},
        )
        
        # Build output context
        output = {
            "validation_passed": result.is_valid,
            "validation_pipeline_id": self._pipeline_id,
            "validation_error_count": result.total_errors,
            "validation_warning_count": result.total_warnings,
        }
        
        if self._include_details:
            output["validation_result"] = result.to_dict()
        
        if result.is_valid:
            output[self._output_key] = result.validated_data or input_data
            self.log_info(
                f"Validation passed: {result.rows_validated} rows validated"
            )
        else:
            self.log_error(
                f"Validation failed: {result.total_errors} errors, "
                f"{result.total_warnings} warnings"
            )
            
            if self._on_failure == "error":
                raise ValidationError(
                    f"Validation pipeline '{self._pipeline_id}' failed with "
                    f"{result.total_errors} errors"
                )
            else:
                # Continue with original data
                output[self._output_key] = input_data
        
        return output


class ValidationError(Exception):
    """Raised when validation fails and on_failure is 'error'."""
    pass
```

### Service Pipeline Configuration Example

```json
{
  "services": {
    "process-customer-data": {
      "steps": [
        {
          "name": "load_reference_data",
          "module": "myproject.components.data_loader",
          "class": "ReferenceDataLoader",
          "config": {
            "datasets": {
              "countries": "./reference/countries.parquet"
            }
          }
        },
        {
          "name": "validate_input",
          "module": "frameworks.data_validation.adapters.pipeline_adapter",
          "class": "DataValidationComponent",
          "config": {
            "pipeline_config_path": "./config/validation_pipelines.json",
            "check_config_path": "./config/custom_checks.json",
            "pipeline_id": "customer_data_validation",
            "input_key": "data",
            "output_key": "validated_data",
            "reference_data_keys": {
              "countries": "countries_ref"
            },
            "on_validation_failure": "error",
            "include_result_details": true
          }
        },
        {
          "name": "transform",
          "module": "myproject.components.transformer",
          "class": "CustomerTransformer",
          "config": {
            "input_key": "validated_data"
          }
        },
        {
          "name": "persist",
          "module": "frameworks.service_pipeline.implementation.components.persistence",
          "class": "PersistenceComponent",
          "config": {
            "output_dir": "./output",
            "format": "parquet"
          }
        }
      ]
    }
  }
}
```

---

## Standalone Usage Examples

### Example 1: Basic Validation

```python
import polars as pl
from frameworks.data_validation import ValidationEngine

# Initialize engine with config files
engine = ValidationEngine(
    pipeline_config_path="./config/validation_pipelines.json",
    check_config_path="./config/custom_checks.json",
)

# Load data
df = pl.read_csv("customers.csv")

# Run validation
result = engine.validate("customer_data_validation", df)

# Check results
if result.is_valid:
    print("Validation passed!")
    print(f"Rows validated: {result.rows_validated}")
else:
    print("Validation failed!")
    print(f"Total errors: {result.total_errors}")
    
    for stage_result in result.stage_results:
        if not stage_result.is_valid:
            print(f"\nStage '{stage_result.stage_name}' errors:")
            for error in stage_result.errors:
                print(f"  - {error.check_name}: {error.error_message}")
```

### Example 2: With Reference Data

```python
import polars as pl
from frameworks.data_validation import ValidationEngine

engine = ValidationEngine(
    pipeline_config_path="./config/validation_pipelines.json",
    check_config_path="./config/custom_checks.json",
)

# Load data and reference datasets
orders_df = pl.read_parquet("orders.parquet")
customers_df = pl.read_parquet("customers.parquet")

# Run validation with reference data
result = engine.validate(
    pipeline_id="order_data_validation",
    data=orders_df,
    context={
        "reference_data": {
            "customers": customers_df,
        }
    }
)

print(f"Valid: {result.is_valid}")
print(f"Execution time: {result.execution_time_ms}ms")
```

### Example 3: Registering Custom Checks Programmatically

```python
import polars as pl
from frameworks.data_validation import ValidationEngine
from frameworks.data_validation.checks.base_check import CustomCheck, CheckResult


class MyCustomCheck(CustomCheck):
    """Custom check for specific business rule."""
    
    def __init__(self, threshold: float):
        self.threshold = threshold
    
    def validate_column(self, df: pl.DataFrame, column: str) -> CheckResult:
        invalid_count = df.filter(pl.col(column) < self.threshold).height
        
        if invalid_count > 0:
            return CheckResult(
                is_valid=False,
                message=f"Found {invalid_count} values below threshold {self.threshold}",
                row_indices=df.with_row_index()
                    .filter(pl.col(column) < self.threshold)["index"]
                    .to_list(),
            )
        
        return CheckResult(is_valid=True, message="All values above threshold")


# Register the custom check
engine = ValidationEngine(
    pipeline_config_path="./config/validation_pipelines.json",
)
engine.register_check("my_custom_check", MyCustomCheck)

# Now the check can be used in pipeline configurations
```

---

## Error Handling

### Failure Modes

| Mode | Behavior |
|------|----------|
| `fail_fast` | Stop execution at first error |
| `collect_all` | Execute all stages, collect all errors |

### Error Categories

| Category | Description |
|----------|-------------|
| `SCHEMA_ERROR` | Data type mismatch, missing columns |
| `CHECK_ERROR` | Value constraint violation |
| `CROSS_FIELD_ERROR` | Cross-column validation failure |
| `REFERENTIAL_ERROR` | Foreign key / reference violation |
| `CONFIGURATION_ERROR` | Invalid pipeline or check configuration |

---

## Configuration File Locations

| File | Purpose | Default Location |
|------|---------|------------------|
| `validation_pipelines.json` | Validation pipeline definitions | `./config/validation_pipelines.json` |
| `custom_checks.json` | Custom check registrations | `./config/custom_checks.json` |

Both JSON and YAML formats are supported. The loader auto-detects format based on file extension.

---

## Dependencies

```
pandera[polars]>=0.18.0
polars>=0.20.0
pyyaml>=6.0
```

---

## Summary

This Data Validation Framework provides:

1. **Four stage types** for different validation needs:
   - `schema_validation` - Pandera-powered schema validation
   - `custom_rules` - Custom business rule checks
   - `cross_field_validation` - Multi-column relationship validation
   - `referential_validation` - Foreign key / reference data validation

2. **Separation of concerns**:
   - Validation pipelines defined in one config file
   - Custom checks defined in a separate config file
   - Clean adapter for Service Pipeline integration

3. **Polars + Pandera integration** for high-performance DataFrame validation

4. **Flexible error handling** with fail-fast or collect-all modes

5. **Dual-use design** - works standalone or as a Service Pipeline step
