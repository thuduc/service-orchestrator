# Data Transformation Framework - Design Document

## 1. Overview

### 1.1 Purpose

The Data Transformation Framework provides a configuration-driven, contract-based system for building and executing data transformation pipelines. It supports both a fluent programmatic API and declarative JSON/YAML configuration, enabling flexible data manipulation workflows using Polars as the underlying DataFrame engine.

### 1.2 Design Goals

1. **Clean and Simple**: Easy to understand and use for both developers and data engineers
2. **Contract-Based**: All components implement well-defined abstract interfaces
3. **Dual Interface**: Support both fluent code API and declarative configuration
4. **Composable**: Transformers are independent building blocks that can be chained
5. **Extensible**: Easy to add custom transformers
6. **Integrated**: Works standalone or as a Service Pipeline component via adapter

### 1.3 Key Features

- Standard set of built-in transformers covering common operations
- Expression parsing for configuration-driven column computations
- Multi-DataFrame support (joins, unions, lookups)
- Type coercion and schema enforcement
- Detailed execution results with timing and row counts
- Full compatibility with existing Service Pipeline framework

## 2. Architecture

### 2.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Public API Layer                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐         │
│  │  Pipeline (Fluent) │  │ TransformationEngine │  │ ServicePipelineAdapter │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
┌─────────────────────────────────────────────────────────────────────────┐
│                         Core Engine Layer                                │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐         │
│  │ TransformerRegistry │  │  ConfigLoader   │  │ ExpressionParser │      │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
┌─────────────────────────────────────────────────────────────────────────┐
│                        Transformer Layer                                 │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐     │
│  │  Select  │ │  Filter  │ │   Join   │ │ GroupBy  │ │   ...    │     │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘     │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
┌─────────────────────────────────────────────────────────────────────────┐
│                         Contract Layer                                   │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐         │
│  │   Transformer   │  │TransformationResult│ │TransformationContext│    │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Directory Structure

```
frameworks/data_transformation/
├── __init__.py                    # Public API exports
├── contract/
│   ├── __init__.py
│   ├── transformer.py             # Transformer ABC
│   └── result.py                  # TransformationResult, StepResult
├── engine/
│   ├── __init__.py
│   ├── transformation_context.py  # TransformationContext dataclass
│   ├── transformation_engine.py   # Main engine orchestration
│   ├── pipeline_builder.py        # Fluent Pipeline builder
│   └── expression_parser.py       # Parse string expressions to Polars
├── registries/
│   ├── __init__.py
│   └── transformer_registry.py    # Registry for transformer classes
├── loaders/
│   ├── __init__.py
│   └── config_loader.py           # JSON/YAML config loading (reuse)
├── transformers/
│   ├── __init__.py
│   ├── base_transformer.py        # BaseTransformer helper class
│   ├── column/
│   │   ├── __init__.py
│   │   ├── select.py              # SelectTransformer
│   │   ├── drop.py                # DropTransformer
│   │   ├── rename.py              # RenameTransformer
│   │   ├── cast.py                # CastTransformer
│   │   └── with_columns.py        # WithColumnsTransformer
│   ├── row/
│   │   ├── __init__.py
│   │   ├── filter.py              # FilterTransformer
│   │   ├── sort.py                # SortTransformer
│   │   ├── unique.py              # UniqueTransformer
│   │   ├── head.py                # HeadTransformer
│   │   ├── tail.py                # TailTransformer
│   │   ├── slice.py               # SliceTransformer
│   │   ├── sample.py              # SampleTransformer
│   │   └── drop_nulls.py          # DropNullsTransformer
│   ├── reshape/
│   │   ├── __init__.py
│   │   ├── pivot.py               # PivotTransformer
│   │   ├── unpivot.py             # UnpivotTransformer
│   │   └── explode.py             # ExplodeTransformer
│   ├── aggregate/
│   │   ├── __init__.py
│   │   └── group_by.py            # GroupByTransformer
│   ├── combine/
│   │   ├── __init__.py
│   │   ├── join.py                # JoinTransformer
│   │   ├── concat.py              # ConcatTransformer (vstack/hstack)
│   │   └── union.py               # UnionTransformer
│   └── fill/
│       ├── __init__.py
│       ├── fill_null.py           # FillNullTransformer
│       └── fill_nan.py            # FillNanTransformer
├── adapters/
│   ├── __init__.py
│   └── pipeline_adapter.py        # DataTransformationComponent
└── tests/
    ├── __init__.py
    ├── conftest.py
    ├── unit/
    │   ├── __init__.py
    │   ├── test_select_transformer.py
    │   ├── test_filter_transformer.py
    │   ├── test_join_transformer.py
    │   ├── ... (one per transformer)
    │   ├── test_transformer_registry.py
    │   ├── test_expression_parser.py
    │   └── test_pipeline_builder.py
    └── integration/
        ├── __init__.py
        └── test_service_pipeline_integration.py
```

## 3. Contract Layer

### 3.1 Transformer Contract

```python
# frameworks/data_transformation/contract/transformer.py

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import polars as pl


class Transformer(ABC):
    """
    Abstract base class for all transformers.
    
    A transformer is a single unit of data transformation that takes
    a DataFrame (and optionally additional context) and produces a
    transformed DataFrame.
    
    Transformers are stateless - all configuration is passed via the
    config dictionary during initialization.
    """
    
    @property
    @abstractmethod
    def transformer_type(self) -> str:
        """
        Unique identifier for this transformer type.
        
        This is used to register the transformer and reference it in
        configuration files.
        
        Returns:
            String identifier (e.g., "select", "filter", "join")
        """
        pass
    
    @property
    def input_type(self) -> str:
        """
        Type of input this transformer expects.
        
        Returns:
            "single" - expects one DataFrame (default)
            "multi" - expects multiple DataFrames (e.g., join, concat)
        """
        return "single"
    
    @abstractmethod
    def transform(
        self,
        data: pl.DataFrame,
        context: "TransformationContext",
    ) -> pl.DataFrame:
        """
        Execute the transformation.
        
        Args:
            data: Primary input DataFrame to transform
            context: Transformation context containing:
                - Additional datasets (for joins, lookups)
                - Metadata and configuration
                - Intermediate results from previous steps
        
        Returns:
            Transformed DataFrame
            
        Raises:
            TransformationError: If transformation fails
        """
        pass
    
    def validate_config(self, config: Dict[str, Any]) -> Optional[str]:
        """
        Validate the transformer configuration.
        
        Override this method to add custom validation logic.
        
        Args:
            config: Configuration dictionary for this transformer
            
        Returns:
            None if valid, error message string if invalid
        """
        return None
    
    def get_required_datasets(self) -> List[str]:
        """
        Return list of additional dataset names required from context.
        
        Override for transformers that need external datasets (e.g., join).
        
        Returns:
            List of dataset names that must be present in context
        """
        return []
```

### 3.2 TransformationContext

```python
# frameworks/data_transformation/engine/transformation_context.py

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import polars as pl


@dataclass
class TransformationContext:
    """
    Context object passed through the transformation pipeline.
    
    Contains the primary data being transformed, additional reference
    datasets, metadata, and intermediate results.
    
    Attributes:
        data: The primary DataFrame being transformed
        datasets: Named additional DataFrames (for joins, lookups, unions)
        metadata: Arbitrary metadata (request_id, timestamps, etc.)
        variables: User-defined variables for expression substitution
        step_results: Results from each completed step (for debugging)
        current_step: Name of the currently executing step
        pipeline_id: ID of the pipeline being executed
    """
    data: pl.DataFrame
    datasets: Dict[str, pl.DataFrame] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    variables: Dict[str, Any] = field(default_factory=dict)
    step_results: List["StepResult"] = field(default_factory=list)
    current_step: Optional[str] = None
    pipeline_id: Optional[str] = None
    
    def get_dataset(self, name: str) -> Optional[pl.DataFrame]:
        """Get a named dataset from context."""
        return self.datasets.get(name)
    
    def set_dataset(self, name: str, df: pl.DataFrame) -> None:
        """Store a named dataset in context."""
        self.datasets[name] = df
    
    def add_step_result(self, result: "StepResult") -> None:
        """Add a step result to the history."""
        self.step_results.append(result)
```

### 3.3 Result Classes

```python
# frameworks/data_transformation/contract/result.py

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import polars as pl


@dataclass
class StepResult:
    """Result from a single transformation step."""
    
    step_name: str
    transformer_type: str
    success: bool
    rows_in: int
    rows_out: int
    columns_in: int
    columns_out: int
    execution_time_ms: float
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "step_name": self.step_name,
            "transformer_type": self.transformer_type,
            "success": self.success,
            "rows_in": self.rows_in,
            "rows_out": self.rows_out,
            "columns_in": self.columns_in,
            "columns_out": self.columns_out,
            "execution_time_ms": self.execution_time_ms,
            "error_message": self.error_message,
        }


@dataclass
class TransformationResult:
    """Aggregated result from a transformation pipeline."""
    
    pipeline_id: str
    success: bool
    data: Optional[pl.DataFrame]  # Final transformed data
    step_results: List[StepResult]
    total_execution_time_ms: float
    rows_in: int
    rows_out: int
    error_message: Optional[str] = None
    
    @property
    def steps_completed(self) -> int:
        """Number of steps that completed successfully."""
        return sum(1 for s in self.step_results if s.success)
    
    @property
    def total_steps(self) -> int:
        """Total number of steps in the pipeline."""
        return len(self.step_results)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "pipeline_id": self.pipeline_id,
            "success": self.success,
            "steps_completed": self.steps_completed,
            "total_steps": self.total_steps,
            "total_execution_time_ms": self.total_execution_time_ms,
            "rows_in": self.rows_in,
            "rows_out": self.rows_out,
            "error_message": self.error_message,
            "step_results": [s.to_dict() for s in self.step_results],
        }
```

## 4. Engine Layer

### 4.1 TransformationEngine

```python
# frameworks/data_transformation/engine/transformation_engine.py

class TransformationEngine:
    """
    Main orchestration engine for transformation pipelines.
    
    Responsibilities:
    - Load pipeline configurations from JSON/YAML
    - Manage transformer registry
    - Execute pipelines step-by-step
    - Aggregate results
    
    Example usage:
        engine = TransformationEngine(
            pipeline_config_path="config/transformations.json"
        )
        
        result = engine.transform(
            pipeline_id="customer_enrichment",
            data=customers_df,
            datasets={"orders": orders_df}
        )
    """
    
    # Built-in transformer types
    BUILTIN_TRANSFORMERS = {
        # Column operations
        "select": SelectTransformer,
        "drop": DropTransformer,
        "rename": RenameTransformer,
        "cast": CastTransformer,
        "with_columns": WithColumnsTransformer,
        # Row operations
        "filter": FilterTransformer,
        "sort": SortTransformer,
        "unique": UniqueTransformer,
        "head": HeadTransformer,
        "tail": TailTransformer,
        "slice": SliceTransformer,
        "sample": SampleTransformer,
        "drop_nulls": DropNullsTransformer,
        # Reshape operations
        "pivot": PivotTransformer,
        "unpivot": UnpivotTransformer,
        "explode": ExplodeTransformer,
        # Aggregate operations
        "group_by": GroupByTransformer,
        # Combine operations
        "join": JoinTransformer,
        "concat": ConcatTransformer,
        "union": UnionTransformer,
        # Fill operations
        "fill_null": FillNullTransformer,
        "fill_nan": FillNanTransformer,
    }
    
    def __init__(
        self,
        pipeline_config_path: Optional[Union[str, Path]] = None,
    ) -> None:
        """
        Initialize the transformation engine.
        
        Args:
            pipeline_config_path: Path to pipeline configuration file
        """
        self._pipelines: Dict[str, Any] = {}
        self._transformer_registry = TransformerRegistry()
        
        # Register built-in transformers
        self._register_builtin_transformers()
        
        # Load configurations
        if pipeline_config_path:
            self._load_pipeline_config(pipeline_config_path)
    
    def transform(
        self,
        pipeline_id: str,
        data: pl.DataFrame,
        datasets: Optional[Dict[str, pl.DataFrame]] = None,
        variables: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> TransformationResult:
        """
        Execute a transformation pipeline.
        
        Args:
            pipeline_id: ID of the pipeline to execute
            data: Primary DataFrame to transform
            datasets: Additional named DataFrames for joins/lookups
            variables: Variables for expression substitution
            metadata: Metadata to include in context
            
        Returns:
            TransformationResult with transformed data and execution info
        """
        pass  # Implementation details in section 4.2
    
    def add_pipeline(
        self,
        pipeline_id: str,
        pipeline_config: Dict[str, Any],
        overwrite: bool = False,
    ) -> None:
        """Add a pipeline configuration programmatically."""
        pass
    
    def register_transformer(
        self,
        transformer_type: str,
        transformer_class: Type[Transformer],
        overwrite: bool = False,
    ) -> None:
        """Register a custom transformer type."""
        pass
```

### 4.2 Pipeline Execution Flow

```
1. Validate pipeline exists
2. Create TransformationContext with input data and datasets
3. For each step in pipeline.steps:
   a. Get transformer class from registry
   b. Instantiate transformer with step config
   c. Validate transformer config
   d. Check required datasets are available
   e. Execute transformer.transform(context.data, context)
   f. Record StepResult (timing, row counts, success/failure)
   g. Update context.data with transformed result
   h. If step fails and on_error="fail_fast": stop pipeline
4. Return TransformationResult with final data and all step results
```

### 4.3 Pipeline Builder (Fluent API)

```python
# frameworks/data_transformation/engine/pipeline_builder.py

class Pipeline:
    """
    Fluent builder for constructing transformation pipelines.
    
    Provides a chainable API for building pipelines programmatically,
    with full IDE autocomplete support.
    
    Example:
        pipeline = (
            Pipeline("my_pipeline")
            .filter(pl.col("status") == "active")
            .join(right="orders", on="customer_id", how="left")
            .group_by(by=["customer_id"], agg={"total": pl.sum("amount")})
            .select(["customer_id", "total"])
        )
        
        result = pipeline.execute(data, datasets={"orders": orders_df})
    """
    
    def __init__(self, pipeline_id: str, description: str = "") -> None:
        """Initialize pipeline builder."""
        self._pipeline_id = pipeline_id
        self._description = description
        self._steps: List[Dict[str, Any]] = []
        self._engine: Optional[TransformationEngine] = None
    
    # Column operations
    def select(self, columns: List[str]) -> "Pipeline":
        """Select specific columns."""
        self._steps.append({
            "name": f"select_{len(self._steps)}",
            "type": "select",
            "config": {"columns": columns}
        })
        return self
    
    def drop(self, columns: List[str]) -> "Pipeline":
        """Drop specific columns."""
        self._steps.append({
            "name": f"drop_{len(self._steps)}",
            "type": "drop",
            "config": {"columns": columns}
        })
        return self
    
    def rename(self, mapping: Dict[str, str]) -> "Pipeline":
        """Rename columns."""
        self._steps.append({
            "name": f"rename_{len(self._steps)}",
            "type": "rename",
            "config": {"mapping": mapping}
        })
        return self
    
    def cast(self, schema: Dict[str, str]) -> "Pipeline":
        """Cast columns to specified types."""
        self._steps.append({
            "name": f"cast_{len(self._steps)}",
            "type": "cast",
            "config": {"schema": schema}
        })
        return self
    
    def with_columns(self, **expressions: Any) -> "Pipeline":
        """Add or modify columns using expressions."""
        self._steps.append({
            "name": f"with_columns_{len(self._steps)}",
            "type": "with_columns",
            "config": {"columns": expressions}
        })
        return self
    
    # Row operations
    def filter(self, condition: Any) -> "Pipeline":
        """Filter rows based on condition."""
        self._steps.append({
            "name": f"filter_{len(self._steps)}",
            "type": "filter",
            "config": {"condition": condition}
        })
        return self
    
    def sort(
        self, 
        by: Union[str, List[str]], 
        descending: Union[bool, List[bool]] = False
    ) -> "Pipeline":
        """Sort rows."""
        self._steps.append({
            "name": f"sort_{len(self._steps)}",
            "type": "sort",
            "config": {"by": by, "descending": descending}
        })
        return self
    
    def unique(
        self, 
        subset: Optional[List[str]] = None,
        keep: str = "first"
    ) -> "Pipeline":
        """Remove duplicate rows."""
        self._steps.append({
            "name": f"unique_{len(self._steps)}",
            "type": "unique",
            "config": {"subset": subset, "keep": keep}
        })
        return self
    
    def head(self, n: int = 5) -> "Pipeline":
        """Take first n rows."""
        self._steps.append({
            "name": f"head_{len(self._steps)}",
            "type": "head",
            "config": {"n": n}
        })
        return self
    
    def tail(self, n: int = 5) -> "Pipeline":
        """Take last n rows."""
        self._steps.append({
            "name": f"tail_{len(self._steps)}",
            "type": "tail",
            "config": {"n": n}
        })
        return self
    
    def slice(self, offset: int, length: int) -> "Pipeline":
        """Slice rows from offset with given length."""
        self._steps.append({
            "name": f"slice_{len(self._steps)}",
            "type": "slice",
            "config": {"offset": offset, "length": length}
        })
        return self
    
    def sample(
        self, 
        n: Optional[int] = None, 
        fraction: Optional[float] = None,
        seed: Optional[int] = None
    ) -> "Pipeline":
        """Sample rows."""
        self._steps.append({
            "name": f"sample_{len(self._steps)}",
            "type": "sample",
            "config": {"n": n, "fraction": fraction, "seed": seed}
        })
        return self
    
    def drop_nulls(self, subset: Optional[List[str]] = None) -> "Pipeline":
        """Drop rows with null values."""
        self._steps.append({
            "name": f"drop_nulls_{len(self._steps)}",
            "type": "drop_nulls",
            "config": {"subset": subset}
        })
        return self
    
    # Reshape operations
    def pivot(
        self,
        on: Union[str, List[str]],
        index: Union[str, List[str]],
        values: Union[str, List[str]],
        aggregate_function: str = "first"
    ) -> "Pipeline":
        """Pivot table."""
        self._steps.append({
            "name": f"pivot_{len(self._steps)}",
            "type": "pivot",
            "config": {
                "on": on,
                "index": index,
                "values": values,
                "aggregate_function": aggregate_function
            }
        })
        return self
    
    def unpivot(
        self,
        on: List[str],
        index: Optional[List[str]] = None,
        variable_name: str = "variable",
        value_name: str = "value"
    ) -> "Pipeline":
        """Unpivot (melt) table."""
        self._steps.append({
            "name": f"unpivot_{len(self._steps)}",
            "type": "unpivot",
            "config": {
                "on": on,
                "index": index,
                "variable_name": variable_name,
                "value_name": value_name
            }
        })
        return self
    
    def explode(self, columns: Union[str, List[str]]) -> "Pipeline":
        """Explode list columns into rows."""
        self._steps.append({
            "name": f"explode_{len(self._steps)}",
            "type": "explode",
            "config": {"columns": columns}
        })
        return self
    
    # Aggregate operations
    def group_by(
        self,
        by: Union[str, List[str]],
        agg: Dict[str, Any]
    ) -> "Pipeline":
        """Group by and aggregate."""
        self._steps.append({
            "name": f"group_by_{len(self._steps)}",
            "type": "group_by",
            "config": {"by": by, "aggregations": agg}
        })
        return self
    
    # Combine operations
    def join(
        self,
        right: str,  # Dataset name from context
        on: Optional[Union[str, List[str]]] = None,
        left_on: Optional[Union[str, List[str]]] = None,
        right_on: Optional[Union[str, List[str]]] = None,
        how: str = "inner",
        suffix: str = "_right"
    ) -> "Pipeline":
        """Join with another dataset."""
        self._steps.append({
            "name": f"join_{len(self._steps)}",
            "type": "join",
            "config": {
                "right_dataset": right,
                "on": on,
                "left_on": left_on,
                "right_on": right_on,
                "how": how,
                "suffix": suffix
            }
        })
        return self
    
    def concat(
        self,
        datasets: List[str],
        how: str = "vertical"  # "vertical" or "horizontal"
    ) -> "Pipeline":
        """Concatenate with other datasets."""
        self._steps.append({
            "name": f"concat_{len(self._steps)}",
            "type": "concat",
            "config": {"datasets": datasets, "how": how}
        })
        return self
    
    def union(self, dataset: str) -> "Pipeline":
        """Union with another dataset (vertical stack + unique)."""
        self._steps.append({
            "name": f"union_{len(self._steps)}",
            "type": "union",
            "config": {"dataset": dataset}
        })
        return self
    
    # Fill operations
    def fill_null(
        self,
        value: Optional[Any] = None,
        strategy: Optional[str] = None,
        columns: Optional[List[str]] = None
    ) -> "Pipeline":
        """Fill null values."""
        self._steps.append({
            "name": f"fill_null_{len(self._steps)}",
            "type": "fill_null",
            "config": {
                "value": value,
                "strategy": strategy,
                "columns": columns
            }
        })
        return self
    
    def fill_nan(
        self,
        value: Any,
        columns: Optional[List[str]] = None
    ) -> "Pipeline":
        """Fill NaN values."""
        self._steps.append({
            "name": f"fill_nan_{len(self._steps)}",
            "type": "fill_nan",
            "config": {"value": value, "columns": columns}
        })
        return self
    
    # Pipeline execution
    def execute(
        self,
        data: pl.DataFrame,
        datasets: Optional[Dict[str, pl.DataFrame]] = None,
        variables: Optional[Dict[str, Any]] = None,
    ) -> TransformationResult:
        """
        Execute the pipeline.
        
        Args:
            data: Primary DataFrame to transform
            datasets: Additional named DataFrames
            variables: Variables for expression substitution
            
        Returns:
            TransformationResult with transformed data
        """
        if self._engine is None:
            self._engine = TransformationEngine()
        
        # Add pipeline to engine
        self._engine.add_pipeline(
            self._pipeline_id,
            {
                "description": self._description,
                "steps": self._steps,
            },
            overwrite=True
        )
        
        return self._engine.transform(
            pipeline_id=self._pipeline_id,
            data=data,
            datasets=datasets,
            variables=variables,
        )
    
    def to_config(self) -> Dict[str, Any]:
        """Export pipeline configuration as dictionary."""
        return {
            "description": self._description,
            "steps": self._steps,
        }
```

### 4.4 Expression Parser

```python
# frameworks/data_transformation/engine/expression_parser.py

class ExpressionParser:
    """
    Parse string expressions into Polars expressions.
    
    Supports a safe subset of Polars expression syntax for use in
    configuration files. Expressions are parsed and validated before
    execution to prevent code injection.
    
    Supported syntax:
        - Column references: col("name"), col('name')
        - Literals: 123, 3.14, "string", True, False, None
        - Arithmetic: +, -, *, /, //, %, **
        - Comparison: ==, !=, <, <=, >, >=
        - Logical: &, |, ~
        - Methods: .is_null(), .is_not_null(), .str.contains(), etc.
        - Functions: sum(), mean(), count(), min(), max(), etc.
        - Conditionals: when(...).then(...).otherwise(...)
        - Aggregations: col("x").sum(), col("x").mean(), etc.
    
    Example:
        parser = ExpressionParser()
        
        # Simple column reference
        expr = parser.parse("col('age')")
        
        # Computed expression
        expr = parser.parse("col('price') * col('quantity')")
        
        # Conditional
        expr = parser.parse(
            "when(col('age') >= 18).then('adult').otherwise('minor')"
        )
    """
    
    # Allowed function names (whitelist for security)
    ALLOWED_FUNCTIONS = {
        # Column reference
        "col", "lit",
        # Aggregations
        "sum", "mean", "avg", "min", "max", "count", "first", "last",
        "std", "var", "median", "quantile", "n_unique",
        # Conditionals
        "when", "then", "otherwise",
        # Type conversion
        "cast",
        # Null handling
        "coalesce", "fill_null",
        # String functions
        "concat", "concat_str",
        # Math functions
        "abs", "sqrt", "exp", "log", "log10",
        "ceil", "floor", "round",
        # Date functions
        "date", "datetime", "duration",
    }
    
    # Allowed methods on expressions
    ALLOWED_METHODS = {
        # Null checks
        "is_null", "is_not_null", "is_nan", "is_not_nan",
        "fill_null", "fill_nan",
        # String methods
        "str", "contains", "starts_with", "ends_with",
        "to_lowercase", "to_uppercase", "strip", "replace",
        "len_chars", "slice",
        # Numeric methods
        "abs", "sqrt", "log", "exp", "round", "floor", "ceil",
        # Date methods
        "dt", "year", "month", "day", "hour", "minute", "second",
        "date", "time", "timestamp",
        # Aggregation methods
        "sum", "mean", "min", "max", "count", "first", "last",
        "std", "var", "median", "n_unique",
        # Comparison
        "eq", "ne", "lt", "le", "gt", "ge",
        # Boolean
        "and_", "or_", "not_",
        # Casting
        "cast",
        # Alias
        "alias",
        # Over (window)
        "over",
    }
    
    def parse(self, expression: str) -> pl.Expr:
        """
        Parse a string expression into a Polars expression.
        
        Args:
            expression: String representation of the expression
            
        Returns:
            Polars expression object
            
        Raises:
            ExpressionParseError: If expression is invalid or uses
                disallowed functions/methods
        """
        pass  # Implementation uses ast.parse with strict validation
    
    def validate(self, expression: str) -> Optional[str]:
        """
        Validate expression without executing it.
        
        Args:
            expression: String expression to validate
            
        Returns:
            None if valid, error message if invalid
        """
        pass
    
    @staticmethod
    def parse_aggregation(agg_spec: Dict[str, Any]) -> pl.Expr:
        """
        Parse an aggregation specification.
        
        Aggregation specs can be:
        - String shorthand: "sum", "mean", "count", etc.
        - Dict with column and agg: {"column": "amount", "agg": "sum"}
        - Full expression string: "col('amount').sum()"
        
        Args:
            agg_spec: Aggregation specification
            
        Returns:
            Polars aggregation expression
        """
        pass
```

### 4.5 Transformer Registry

```python
# frameworks/data_transformation/registries/transformer_registry.py

class TransformerRegistry:
    """
    Registry for transformer classes.
    
    Manages the mapping between transformer type identifiers and their
    implementing classes. Supports both built-in and custom transformers.
    """
    
    def __init__(self) -> None:
        self._transformers: Dict[str, Type[Transformer]] = {}
    
    def register(
        self,
        transformer_type: str,
        transformer_class: Type[Transformer],
        overwrite: bool = False,
    ) -> None:
        """
        Register a transformer class.
        
        Args:
            transformer_type: Unique identifier for this transformer
            transformer_class: Class implementing Transformer ABC
            overwrite: If True, replace existing registration
            
        Raises:
            ValueError: If transformer_type already registered and not overwrite
            TypeError: If transformer_class doesn't implement Transformer
        """
        if transformer_type in self._transformers and not overwrite:
            raise ValueError(
                f"Transformer '{transformer_type}' already registered"
            )
        
        if not (isinstance(transformer_class, type) and 
                issubclass(transformer_class, Transformer)):
            raise TypeError(
                f"Expected Transformer subclass, got {type(transformer_class)}"
            )
        
        self._transformers[transformer_type] = transformer_class
    
    def get(self, transformer_type: str) -> Optional[Type[Transformer]]:
        """Get a transformer class by type."""
        return self._transformers.get(transformer_type)
    
    def has(self, transformer_type: str) -> bool:
        """Check if a transformer type is registered."""
        return transformer_type in self._transformers
    
    def list_transformers(self) -> List[str]:
        """List all registered transformer types."""
        return list(self._transformers.keys())
    
    def unregister(self, transformer_type: str) -> None:
        """Remove a transformer registration."""
        if transformer_type not in self._transformers:
            raise KeyError(f"Transformer '{transformer_type}' not registered")
        del self._transformers[transformer_type]
    
    def clear(self) -> None:
        """Remove all registrations."""
        self._transformers.clear()
    
    def __contains__(self, transformer_type: str) -> bool:
        return self.has(transformer_type)
    
    def __len__(self) -> int:
        return len(self._transformers)
```

## 5. Transformer Implementations

### 5.1 Base Transformer

```python
# frameworks/data_transformation/transformers/base_transformer.py

class BaseTransformer(Transformer):
    """
    Base class for transformer implementations.
    
    Provides common utilities and default implementations.
    Subclasses should override transformer_type and transform().
    """
    
    def __init__(self, name: str, config: Dict[str, Any]) -> None:
        """
        Initialize the transformer.
        
        Args:
            name: Instance name for this transformer step
            config: Configuration dictionary
        """
        self.name = name
        self.config = config
    
    @property
    def transformer_type(self) -> str:
        raise NotImplementedError("Subclasses must define transformer_type")
    
    def _get_required(self, key: str, type_: type = str) -> Any:
        """Get a required config value with type checking."""
        if key not in self.config:
            raise ConfigurationError(
                f"{self.transformer_type}: missing required config '{key}'"
            )
        value = self.config[key]
        if not isinstance(value, type_):
            raise ConfigurationError(
                f"{self.transformer_type}: '{key}' must be {type_.__name__}"
            )
        return value
    
    def _get_optional(
        self, 
        key: str, 
        default: Any = None,
        type_: Optional[type] = None
    ) -> Any:
        """Get an optional config value with type checking."""
        value = self.config.get(key, default)
        if value is not None and type_ is not None:
            if not isinstance(value, type_):
                raise ConfigurationError(
                    f"{self.transformer_type}: '{key}' must be {type_.__name__}"
                )
        return value
```

### 5.2 Column Transformers

#### SelectTransformer
```python
# frameworks/data_transformation/transformers/column/select.py

class SelectTransformer(BaseTransformer):
    """
    Select specific columns from the DataFrame.
    
    Configuration:
        columns: List of column names to select
        
    Example config:
        {"columns": ["id", "name", "email"]}
    """
    
    transformer_type = "select"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext
    ) -> pl.DataFrame:
        columns = self._get_required("columns", list)
        return data.select(columns)
    
    def validate_config(self, config: Dict[str, Any]) -> Optional[str]:
        if "columns" not in config:
            return "SelectTransformer requires 'columns' configuration"
        if not isinstance(config["columns"], list):
            return "'columns' must be a list"
        if len(config["columns"]) == 0:
            return "'columns' cannot be empty"
        return None
```

#### DropTransformer
```python
# frameworks/data_transformation/transformers/column/drop.py

class DropTransformer(BaseTransformer):
    """
    Drop specific columns from the DataFrame.
    
    Configuration:
        columns: List of column names to drop
        
    Example config:
        {"columns": ["temp_col", "debug_info"]}
    """
    
    transformer_type = "drop"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext
    ) -> pl.DataFrame:
        columns = self._get_required("columns", list)
        return data.drop(columns)
```

#### RenameTransformer
```python
# frameworks/data_transformation/transformers/column/rename.py

class RenameTransformer(BaseTransformer):
    """
    Rename columns in the DataFrame.
    
    Configuration:
        mapping: Dictionary mapping old names to new names
        
    Example config:
        {"mapping": {"old_name": "new_name", "col1": "column_1"}}
    """
    
    transformer_type = "rename"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext
    ) -> pl.DataFrame:
        mapping = self._get_required("mapping", dict)
        return data.rename(mapping)
```

#### CastTransformer
```python
# frameworks/data_transformation/transformers/column/cast.py

class CastTransformer(BaseTransformer):
    """
    Cast columns to specified data types.
    
    Configuration:
        schema: Dictionary mapping column names to target types
        strict: If True, raise on cast failure (default: True)
        
    Supported types: Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64,
                     Float32, Float64, Boolean, Utf8, Date, Datetime, Time
        
    Example config:
        {"schema": {"age": "Int64", "price": "Float64", "active": "Boolean"}}
    """
    
    transformer_type = "cast"
    
    TYPE_MAP = {
        "Int8": pl.Int8, "Int16": pl.Int16, "Int32": pl.Int32, "Int64": pl.Int64,
        "UInt8": pl.UInt8, "UInt16": pl.UInt16, "UInt32": pl.UInt32, "UInt64": pl.UInt64,
        "Float32": pl.Float32, "Float64": pl.Float64,
        "Boolean": pl.Boolean, "Bool": pl.Boolean,
        "Utf8": pl.Utf8, "String": pl.Utf8,
        "Date": pl.Date, "Datetime": pl.Datetime, "Time": pl.Time,
    }
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext
    ) -> pl.DataFrame:
        schema = self._get_required("schema", dict)
        strict = self._get_optional("strict", True, bool)
        
        cast_exprs = []
        for col_name, type_name in schema.items():
            if type_name not in self.TYPE_MAP:
                raise ConfigurationError(
                    f"Unknown type '{type_name}' for column '{col_name}'"
                )
            cast_exprs.append(
                pl.col(col_name).cast(self.TYPE_MAP[type_name], strict=strict)
            )
        
        return data.with_columns(cast_exprs)
```

#### WithColumnsTransformer
```python
# frameworks/data_transformation/transformers/column/with_columns.py

class WithColumnsTransformer(BaseTransformer):
    """
    Add or modify columns using expressions.
    
    Configuration:
        columns: Dictionary mapping column names to expressions
        
    Expressions can be:
        - String expression: "col('price') * col('quantity')"
        - Literal value: 100, "default", True
        
    Example config:
        {
            "columns": {
                "full_name": "col('first_name') + ' ' + col('last_name')",
                "total": "col('price') * col('quantity')",
                "status": "'active'"
            }
        }
    """
    
    transformer_type = "with_columns"
    
    def __init__(self, name: str, config: Dict[str, Any]) -> None:
        super().__init__(name, config)
        self._parser = ExpressionParser()
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext
    ) -> pl.DataFrame:
        columns = self._get_required("columns", dict)
        
        exprs = []
        for col_name, expr_def in columns.items():
            if isinstance(expr_def, str):
                expr = self._parser.parse(expr_def)
            elif isinstance(expr_def, pl.Expr):
                expr = expr_def
            else:
                # Literal value
                expr = pl.lit(expr_def)
            exprs.append(expr.alias(col_name))
        
        return data.with_columns(exprs)
```

### 5.3 Row Transformers

#### FilterTransformer
```python
# frameworks/data_transformation/transformers/row/filter.py

class FilterTransformer(BaseTransformer):
    """
    Filter rows based on a condition.
    
    Configuration:
        condition: Filter condition (string expression or Polars expression)
        
    Example configs:
        {"condition": "col('age') >= 18"}
        {"condition": "col('status') == 'active'"}
        {"condition": "col('amount').is_not_null()"}
    """
    
    transformer_type = "filter"
    
    def __init__(self, name: str, config: Dict[str, Any]) -> None:
        super().__init__(name, config)
        self._parser = ExpressionParser()
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext
    ) -> pl.DataFrame:
        condition = self._get_required("condition")
        
        if isinstance(condition, str):
            expr = self._parser.parse(condition)
        elif isinstance(condition, pl.Expr):
            expr = condition
        else:
            raise ConfigurationError(
                "filter condition must be string or Polars expression"
            )
        
        return data.filter(expr)
```

#### SortTransformer
```python
# frameworks/data_transformation/transformers/row/sort.py

class SortTransformer(BaseTransformer):
    """
    Sort rows by one or more columns.
    
    Configuration:
        by: Column name or list of column names to sort by
        descending: Boolean or list of booleans for sort direction
        nulls_last: If True, nulls appear last (default: False)
        
    Example configs:
        {"by": "date", "descending": true}
        {"by": ["category", "date"], "descending": [false, true]}
    """
    
    transformer_type = "sort"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext
    ) -> pl.DataFrame:
        by = self._get_required("by")
        descending = self._get_optional("descending", False)
        nulls_last = self._get_optional("nulls_last", False, bool)
        
        # Normalize to lists
        if isinstance(by, str):
            by = [by]
        if isinstance(descending, bool):
            descending = [descending] * len(by)
        
        return data.sort(by, descending=descending, nulls_last=nulls_last)
```

#### UniqueTransformer
```python
# frameworks/data_transformation/transformers/row/unique.py

class UniqueTransformer(BaseTransformer):
    """
    Remove duplicate rows.
    
    Configuration:
        subset: List of columns to consider for uniqueness (default: all)
        keep: Which duplicate to keep: "first", "last", "any", "none"
        maintain_order: Maintain original row order (default: True)
        
    Example configs:
        {"subset": ["customer_id"], "keep": "last"}
        {"keep": "first"}
    """
    
    transformer_type = "unique"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext
    ) -> pl.DataFrame:
        subset = self._get_optional("subset", None)
        keep = self._get_optional("keep", "first", str)
        maintain_order = self._get_optional("maintain_order", True, bool)
        
        return data.unique(
            subset=subset,
            keep=keep,
            maintain_order=maintain_order
        )
```

#### HeadTransformer
```python
# frameworks/data_transformation/transformers/row/head.py

class HeadTransformer(BaseTransformer):
    """
    Take the first n rows.
    
    Configuration:
        n: Number of rows to take (default: 5)
        
    Example config:
        {"n": 100}
    """
    
    transformer_type = "head"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext
    ) -> pl.DataFrame:
        n = self._get_optional("n", 5, int)
        return data.head(n)
```

#### TailTransformer
```python
# frameworks/data_transformation/transformers/row/tail.py

class TailTransformer(BaseTransformer):
    """
    Take the last n rows.
    
    Configuration:
        n: Number of rows to take (default: 5)
        
    Example config:
        {"n": 100}
    """
    
    transformer_type = "tail"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext
    ) -> pl.DataFrame:
        n = self._get_optional("n", 5, int)
        return data.tail(n)
```

#### SliceTransformer
```python
# frameworks/data_transformation/transformers/row/slice.py

class SliceTransformer(BaseTransformer):
    """
    Slice rows from a given offset with a given length.
    
    Configuration:
        offset: Starting row index
        length: Number of rows to take
        
    Example config:
        {"offset": 10, "length": 50}
    """
    
    transformer_type = "slice"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext
    ) -> pl.DataFrame:
        offset = self._get_required("offset", int)
        length = self._get_required("length", int)
        return data.slice(offset, length)
```

#### SampleTransformer
```python
# frameworks/data_transformation/transformers/row/sample.py

class SampleTransformer(BaseTransformer):
    """
    Sample rows from the DataFrame.
    
    Configuration:
        n: Number of rows to sample (mutually exclusive with fraction)
        fraction: Fraction of rows to sample (mutually exclusive with n)
        with_replacement: Allow sampling same row multiple times (default: False)
        shuffle: Shuffle the result (default: True)
        seed: Random seed for reproducibility
        
    Example configs:
        {"n": 100, "seed": 42}
        {"fraction": 0.1}
    """
    
    transformer_type = "sample"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext
    ) -> pl.DataFrame:
        n = self._get_optional("n", None)
        fraction = self._get_optional("fraction", None)
        with_replacement = self._get_optional("with_replacement", False, bool)
        shuffle = self._get_optional("shuffle", True, bool)
        seed = self._get_optional("seed", None)
        
        if n is None and fraction is None:
            raise ConfigurationError("sample requires either 'n' or 'fraction'")
        if n is not None and fraction is not None:
            raise ConfigurationError("sample cannot have both 'n' and 'fraction'")
        
        return data.sample(
            n=n,
            fraction=fraction,
            with_replacement=with_replacement,
            shuffle=shuffle,
            seed=seed
        )
```

#### DropNullsTransformer
```python
# frameworks/data_transformation/transformers/row/drop_nulls.py

class DropNullsTransformer(BaseTransformer):
    """
    Drop rows with null values.
    
    Configuration:
        subset: List of columns to check for nulls (default: all columns)
        
    Example configs:
        {"subset": ["required_field_1", "required_field_2"]}
        {}  # Check all columns
    """
    
    transformer_type = "drop_nulls"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext
    ) -> pl.DataFrame:
        subset = self._get_optional("subset", None)
        return data.drop_nulls(subset=subset)
```

### 5.4 Reshape Transformers

#### PivotTransformer
```python
# frameworks/data_transformation/transformers/reshape/pivot.py

class PivotTransformer(BaseTransformer):
    """
    Pivot table from long to wide format.
    
    Configuration:
        on: Column(s) whose values become new column names
        index: Column(s) to use as row identifiers
        values: Column(s) containing values to fill the matrix
        aggregate_function: How to aggregate if multiple values
                           ("first", "sum", "mean", "min", "max", "count")
        
    Example config:
        {
            "on": "month",
            "index": "product",
            "values": "sales",
            "aggregate_function": "sum"
        }
    """
    
    transformer_type = "pivot"
    
    AGGREGATE_FUNCTIONS = {
        "first": "first",
        "last": "last",
        "sum": "sum",
        "mean": "mean",
        "min": "min",
        "max": "max",
        "count": "count",
        "len": "len",
    }
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext
    ) -> pl.DataFrame:
        on = self._get_required("on")
        index = self._get_required("index")
        values = self._get_required("values")
        agg_fn = self._get_optional("aggregate_function", "first", str)
        
        if agg_fn not in self.AGGREGATE_FUNCTIONS:
            raise ConfigurationError(
                f"Unknown aggregate_function '{agg_fn}'. "
                f"Allowed: {list(self.AGGREGATE_FUNCTIONS.keys())}"
            )
        
        return data.pivot(
            on=on,
            index=index,
            values=values,
            aggregate_function=agg_fn
        )
```

#### UnpivotTransformer
```python
# frameworks/data_transformation/transformers/reshape/unpivot.py

class UnpivotTransformer(BaseTransformer):
    """
    Unpivot (melt) table from wide to long format.
    
    Configuration:
        on: Column(s) to unpivot (their names become values)
        index: Column(s) to keep as identifiers (optional)
        variable_name: Name for the new column containing original column names
        value_name: Name for the new column containing values
        
    Example config:
        {
            "on": ["jan_sales", "feb_sales", "mar_sales"],
            "index": ["product_id", "product_name"],
            "variable_name": "month",
            "value_name": "sales"
        }
    """
    
    transformer_type = "unpivot"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext
    ) -> pl.DataFrame:
        on = self._get_required("on", list)
        index = self._get_optional("index", None)
        variable_name = self._get_optional("variable_name", "variable", str)
        value_name = self._get_optional("value_name", "value", str)
        
        return data.unpivot(
            on=on,
            index=index,
            variable_name=variable_name,
            value_name=value_name
        )
```

#### ExplodeTransformer
```python
# frameworks/data_transformation/transformers/reshape/explode.py

class ExplodeTransformer(BaseTransformer):
    """
    Explode list columns into separate rows.
    
    Configuration:
        columns: Column name or list of columns to explode
        
    Example config:
        {"columns": "tags"}
        {"columns": ["tags", "categories"]}
    """
    
    transformer_type = "explode"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext
    ) -> pl.DataFrame:
        columns = self._get_required("columns")
        
        # Normalize to list
        if isinstance(columns, str):
            columns = [columns]
        
        return data.explode(columns)
```

### 5.5 Aggregate Transformers

#### GroupByTransformer
```python
# frameworks/data_transformation/transformers/aggregate/group_by.py

class GroupByTransformer(BaseTransformer):
    """
    Group by columns and aggregate.
    
    Configuration:
        by: Column(s) to group by
        aggregations: Dictionary of output columns to aggregation specs
        maintain_order: Maintain order of groups (default: True)
        
    Aggregation specs can be:
        - String shorthand: "sum", "mean", "count", "min", "max", etc.
        - Dict: {"column": "amount", "agg": "sum"}
        - Expression string: "col('amount').sum()"
        
    Example config:
        {
            "by": ["customer_id"],
            "aggregations": {
                "total_orders": "count",
                "total_spent": {"column": "amount", "agg": "sum"},
                "avg_order": {"column": "amount", "agg": "mean"},
                "first_order": {"column": "order_date", "agg": "min"},
                "last_order": {"column": "order_date", "agg": "max"}
            }
        }
    """
    
    transformer_type = "group_by"
    
    AGGREGATION_FUNCTIONS = {
        "sum": lambda c: pl.col(c).sum(),
        "mean": lambda c: pl.col(c).mean(),
        "avg": lambda c: pl.col(c).mean(),
        "min": lambda c: pl.col(c).min(),
        "max": lambda c: pl.col(c).max(),
        "count": lambda c: pl.col(c).count(),
        "n_unique": lambda c: pl.col(c).n_unique(),
        "first": lambda c: pl.col(c).first(),
        "last": lambda c: pl.col(c).last(),
        "std": lambda c: pl.col(c).std(),
        "var": lambda c: pl.col(c).var(),
        "median": lambda c: pl.col(c).median(),
    }
    
    def __init__(self, name: str, config: Dict[str, Any]) -> None:
        super().__init__(name, config)
        self._parser = ExpressionParser()
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext
    ) -> pl.DataFrame:
        by = self._get_required("by")
        aggregations = self._get_required("aggregations", dict)
        maintain_order = self._get_optional("maintain_order", True, bool)
        
        # Normalize by to list
        if isinstance(by, str):
            by = [by]
        
        # Build aggregation expressions
        agg_exprs = []
        for output_name, agg_spec in aggregations.items():
            expr = self._parse_aggregation(agg_spec, output_name)
            agg_exprs.append(expr)
        
        return data.group_by(by, maintain_order=maintain_order).agg(agg_exprs)
    
    def _parse_aggregation(
        self, 
        agg_spec: Any, 
        output_name: str
    ) -> pl.Expr:
        """Parse an aggregation specification into a Polars expression."""
        if isinstance(agg_spec, str):
            # Check if it's a shorthand (just the function name)
            if agg_spec in self.AGGREGATION_FUNCTIONS:
                # For shorthand, use the output name as the column
                return self.AGGREGATION_FUNCTIONS[agg_spec](output_name).alias(output_name)
            else:
                # Try parsing as expression
                return self._parser.parse(agg_spec).alias(output_name)
        
        elif isinstance(agg_spec, dict):
            column = agg_spec.get("column")
            agg = agg_spec.get("agg")
            
            if column is None or agg is None:
                raise ConfigurationError(
                    f"Aggregation dict must have 'column' and 'agg' keys"
                )
            
            if agg not in self.AGGREGATION_FUNCTIONS:
                raise ConfigurationError(
                    f"Unknown aggregation '{agg}'. "
                    f"Allowed: {list(self.AGGREGATION_FUNCTIONS.keys())}"
                )
            
            return self.AGGREGATION_FUNCTIONS[agg](column).alias(output_name)
        
        elif isinstance(agg_spec, pl.Expr):
            return agg_spec.alias(output_name)
        
        else:
            raise ConfigurationError(
                f"Invalid aggregation spec type: {type(agg_spec)}"
            )
```

### 5.6 Combine Transformers

#### JoinTransformer
```python
# frameworks/data_transformation/transformers/combine/join.py

class JoinTransformer(BaseTransformer):
    """
    Join with another DataFrame from context.
    
    Configuration:
        right_dataset: Name of the dataset in context to join with
        on: Column(s) to join on (if same name in both)
        left_on: Column(s) from left DataFrame
        right_on: Column(s) from right DataFrame
        how: Join type ("inner", "left", "right", "outer", "semi", "anti", "cross")
        suffix: Suffix for duplicate column names from right (default: "_right")
        
    Example configs:
        {"right_dataset": "orders", "on": "customer_id", "how": "left"}
        {
            "right_dataset": "products",
            "left_on": "product_code",
            "right_on": "code",
            "how": "inner"
        }
    """
    
    transformer_type = "join"
    input_type = "multi"
    
    VALID_JOIN_TYPES = {"inner", "left", "right", "outer", "semi", "anti", "cross"}
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext
    ) -> pl.DataFrame:
        right_dataset = self._get_required("right_dataset", str)
        on = self._get_optional("on", None)
        left_on = self._get_optional("left_on", None)
        right_on = self._get_optional("right_on", None)
        how = self._get_optional("how", "inner", str)
        suffix = self._get_optional("suffix", "_right", str)
        
        # Validate join type
        if how not in self.VALID_JOIN_TYPES:
            raise ConfigurationError(
                f"Invalid join type '{how}'. Allowed: {self.VALID_JOIN_TYPES}"
            )
        
        # Get right dataset from context
        right_df = context.get_dataset(right_dataset)
        if right_df is None:
            raise TransformationError(
                f"Dataset '{right_dataset}' not found in context"
            )
        
        # Validate join keys
        if on is None and (left_on is None or right_on is None):
            if how != "cross":
                raise ConfigurationError(
                    "Join requires either 'on' or both 'left_on' and 'right_on'"
                )
        
        return data.join(
            right_df,
            on=on,
            left_on=left_on,
            right_on=right_on,
            how=how,
            suffix=suffix
        )
    
    def get_required_datasets(self) -> List[str]:
        return [self.config.get("right_dataset")]
    
    def validate_config(self, config: Dict[str, Any]) -> Optional[str]:
        if "right_dataset" not in config:
            return "JoinTransformer requires 'right_dataset'"
        return None
```

#### ConcatTransformer
```python
# frameworks/data_transformation/transformers/combine/concat.py

class ConcatTransformer(BaseTransformer):
    """
    Concatenate with other DataFrames from context.
    
    Configuration:
        datasets: List of dataset names to concatenate
        how: "vertical" (stack rows) or "horizontal" (stack columns)
        rechunk: Reallocate memory after concat (default: True)
        
    Example configs:
        {"datasets": ["df2", "df3"], "how": "vertical"}
        {"datasets": ["features"], "how": "horizontal"}
    """
    
    transformer_type = "concat"
    input_type = "multi"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext
    ) -> pl.DataFrame:
        datasets = self._get_required("datasets", list)
        how = self._get_optional("how", "vertical", str)
        rechunk = self._get_optional("rechunk", True, bool)
        
        # Collect DataFrames
        dfs = [data]
        for ds_name in datasets:
            df = context.get_dataset(ds_name)
            if df is None:
                raise TransformationError(
                    f"Dataset '{ds_name}' not found in context"
                )
            dfs.append(df)
        
        if how == "vertical":
            return pl.concat(dfs, how="vertical", rechunk=rechunk)
        elif how == "horizontal":
            return pl.concat(dfs, how="horizontal", rechunk=rechunk)
        else:
            raise ConfigurationError(
                f"Invalid concat direction '{how}'. Use 'vertical' or 'horizontal'"
            )
    
    def get_required_datasets(self) -> List[str]:
        return self.config.get("datasets", [])
```

#### UnionTransformer
```python
# frameworks/data_transformation/transformers/combine/union.py

class UnionTransformer(BaseTransformer):
    """
    Union with another DataFrame (vertical concat + unique).
    
    This is equivalent to SQL UNION (not UNION ALL).
    
    Configuration:
        dataset: Name of dataset to union with
        
    Example config:
        {"dataset": "additional_records"}
    """
    
    transformer_type = "union"
    input_type = "multi"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext
    ) -> pl.DataFrame:
        dataset = self._get_required("dataset", str)
        
        other_df = context.get_dataset(dataset)
        if other_df is None:
            raise TransformationError(
                f"Dataset '{dataset}' not found in context"
            )
        
        # Concat then unique
        return pl.concat([data, other_df], how="vertical").unique()
    
    def get_required_datasets(self) -> List[str]:
        return [self.config.get("dataset")]
```

### 5.7 Fill Transformers

#### FillNullTransformer
```python
# frameworks/data_transformation/transformers/fill/fill_null.py

class FillNullTransformer(BaseTransformer):
    """
    Fill null values in columns.
    
    Configuration:
        value: Literal value to fill nulls with
        strategy: Fill strategy ("forward", "backward", "min", "max", "mean", "zero", "one")
        columns: List of columns to fill (default: all columns with nulls)
        
    Note: Use either 'value' or 'strategy', not both.
        
    Example configs:
        {"value": 0}
        {"strategy": "forward", "columns": ["price"]}
        {"value": "unknown", "columns": ["category"]}
    """
    
    transformer_type = "fill_null"
    
    VALID_STRATEGIES = {"forward", "backward", "min", "max", "mean", "zero", "one"}
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext
    ) -> pl.DataFrame:
        value = self._get_optional("value", None)
        strategy = self._get_optional("strategy", None)
        columns = self._get_optional("columns", None)
        
        if value is None and strategy is None:
            raise ConfigurationError(
                "fill_null requires either 'value' or 'strategy'"
            )
        
        if value is not None and strategy is not None:
            raise ConfigurationError(
                "fill_null cannot have both 'value' and 'strategy'"
            )
        
        if strategy is not None and strategy not in self.VALID_STRATEGIES:
            raise ConfigurationError(
                f"Invalid strategy '{strategy}'. Allowed: {self.VALID_STRATEGIES}"
            )
        
        if columns is not None:
            # Fill specific columns
            if value is not None:
                exprs = [pl.col(c).fill_null(value) for c in columns]
            else:
                exprs = [pl.col(c).fill_null(strategy=strategy) for c in columns]
            return data.with_columns(exprs)
        else:
            # Fill all columns
            if value is not None:
                return data.fill_null(value)
            else:
                return data.fill_null(strategy=strategy)
```

#### FillNanTransformer
```python
# frameworks/data_transformation/transformers/fill/fill_nan.py

class FillNanTransformer(BaseTransformer):
    """
    Fill NaN values in floating-point columns.
    
    Configuration:
        value: Value to replace NaN with
        columns: List of columns to fill (default: all float columns)
        
    Example configs:
        {"value": 0.0}
        {"value": 0.0, "columns": ["temperature", "humidity"]}
    """
    
    transformer_type = "fill_nan"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext
    ) -> pl.DataFrame:
        value = self._get_required("value")
        columns = self._get_optional("columns", None)
        
        if columns is not None:
            exprs = [pl.col(c).fill_nan(value) for c in columns]
            return data.with_columns(exprs)
        else:
            return data.fill_nan(value)
```

## 6. Configuration Format

### 6.1 Pipeline Configuration Schema

Pipeline configurations can be stored in JSON or YAML format. The configuration loader (reused from the validation framework) handles both formats transparently.

#### JSON Configuration Example
```json
{
  "pipelines": {
    "customer_enrichment": {
      "description": "Enrich customer data with order statistics",
      "steps": [
        {
          "name": "filter_active",
          "type": "filter",
          "config": {
            "condition": "col('status') == 'active'"
          }
        },
        {
          "name": "join_orders",
          "type": "join",
          "config": {
            "right_dataset": "orders",
            "on": "customer_id",
            "how": "left"
          }
        },
        {
          "name": "calculate_totals",
          "type": "group_by",
          "config": {
            "by": ["customer_id", "name", "email"],
            "aggregations": {
              "total_orders": "count",
              "total_spent": {"column": "amount", "agg": "sum"},
              "avg_order": {"column": "amount", "agg": "mean"}
            }
          }
        },
        {
          "name": "add_tier",
          "type": "with_columns",
          "config": {
            "columns": {
              "tier": "when(col('total_spent') >= 1000).then('gold').when(col('total_spent') >= 500).then('silver').otherwise('bronze')"
            }
          }
        },
        {
          "name": "select_final",
          "type": "select",
          "config": {
            "columns": ["customer_id", "name", "email", "total_orders", "total_spent", "avg_order", "tier"]
          }
        }
      ]
    },
    
    "sales_summary": {
      "description": "Generate monthly sales summary by region",
      "steps": [
        {
          "name": "filter_complete",
          "type": "filter",
          "config": {
            "condition": "col('order_status') == 'completed'"
          }
        },
        {
          "name": "add_month",
          "type": "with_columns",
          "config": {
            "columns": {
              "month": "col('order_date').dt.strftime('%Y-%m')"
            }
          }
        },
        {
          "name": "aggregate_by_region_month",
          "type": "group_by",
          "config": {
            "by": ["region", "month"],
            "aggregations": {
              "total_sales": {"column": "amount", "agg": "sum"},
              "order_count": {"column": "order_id", "agg": "count"},
              "avg_order_value": {"column": "amount", "agg": "mean"}
            }
          }
        },
        {
          "name": "sort_results",
          "type": "sort",
          "config": {
            "by": ["region", "month"],
            "descending": [false, false]
          }
        }
      ]
    }
  }
}
```

#### YAML Configuration Example
```yaml
pipelines:
  customer_enrichment:
    description: Enrich customer data with order statistics
    steps:
      - name: filter_active
        type: filter
        config:
          condition: "col('status') == 'active'"
      
      - name: join_orders
        type: join
        config:
          right_dataset: orders
          on: customer_id
          how: left
      
      - name: calculate_totals
        type: group_by
        config:
          by:
            - customer_id
            - name
            - email
          aggregations:
            total_orders: count
            total_spent:
              column: amount
              agg: sum
            avg_order:
              column: amount
              agg: mean
      
      - name: add_tier
        type: with_columns
        config:
          columns:
            tier: >-
              when(col('total_spent') >= 1000).then('gold')
              .when(col('total_spent') >= 500).then('silver')
              .otherwise('bronze')
      
      - name: select_final
        type: select
        config:
          columns:
            - customer_id
            - name
            - email
            - total_orders
            - total_spent
            - avg_order
            - tier

  data_cleaning:
    description: Clean and standardize raw data
    steps:
      - name: drop_nulls_required
        type: drop_nulls
        config:
          subset:
            - id
            - created_at
      
      - name: fill_defaults
        type: fill_null
        config:
          value: "unknown"
          columns:
            - category
            - source
      
      - name: cast_types
        type: cast
        config:
          schema:
            id: Int64
            amount: Float64
            is_active: Boolean
      
      - name: rename_columns
        type: rename
        config:
          mapping:
            old_id: id
            old_name: name
      
      - name: remove_duplicates
        type: unique
        config:
          subset:
            - id
          keep: last
```

### 6.2 Config Loader

```python
# frameworks/data_transformation/loaders/config_loader.py

import json
import yaml
from pathlib import Path
from typing import Any, Dict, Union


class ConfigLoader:
    """
    Load transformation pipeline configurations from JSON or YAML files.
    
    Reuses the pattern from the validation framework for consistency.
    """
    
    @staticmethod
    def load(path: Union[str, Path]) -> Dict[str, Any]:
        """
        Load configuration from a file.
        
        Args:
            path: Path to JSON or YAML configuration file
            
        Returns:
            Parsed configuration dictionary
            
        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If file format is not supported
            json.JSONDecodeError: If JSON is invalid
            yaml.YAMLError: If YAML is invalid
        """
        path = Path(path)
        
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {path}")
        
        suffix = path.suffix.lower()
        content = path.read_text()
        
        if suffix == ".json":
            return json.loads(content)
        elif suffix in (".yaml", ".yml"):
            return yaml.safe_load(content)
        else:
            raise ValueError(
                f"Unsupported configuration format: {suffix}. "
                "Use .json, .yaml, or .yml"
            )
    
    @staticmethod
    def load_string(content: str, format: str = "json") -> Dict[str, Any]:
        """
        Load configuration from a string.
        
        Args:
            content: Configuration content as string
            format: Format of the content ("json" or "yaml")
            
        Returns:
            Parsed configuration dictionary
        """
        if format == "json":
            return json.loads(content)
        elif format in ("yaml", "yml"):
            return yaml.safe_load(content)
        else:
            raise ValueError(f"Unsupported format: {format}")
```

## 7. Service Pipeline Adapter

### 7.1 DataTransformationComponent

```python
# frameworks/data_transformation/adapters/pipeline_adapter.py

from typing import Any, Dict, List, Optional, Union
from pathlib import Path

import polars as pl

from frameworks.service_pipeline.contracts import (
    PipelineComponent,
    PipelineContext,
    PipelineResult,
)
from frameworks.data_transformation.engine.transformation_engine import (
    TransformationEngine,
)
from frameworks.data_transformation.contract.result import TransformationResult


class DataTransformationComponent(PipelineComponent):
    """
    Service Pipeline component for data transformation.
    
    Wraps the TransformationEngine to integrate with the Service Pipeline
    framework, providing configuration-driven data transformation as a
    pipeline component.
    
    Configuration options:
        pipeline_id: ID of the transformation pipeline to execute
        input_key: Key in context.data containing input DataFrame (default: "data")
        output_key: Key to store transformed DataFrame (default: "data")
        datasets_key: Key containing additional datasets dict (default: "datasets")
        variables_key: Key containing variables dict (default: "variables")
        fail_on_error: Whether to fail the pipeline on transformation error (default: True)
        
    Example usage in Service Pipeline config:
        {
            "pipeline": {
                "components": [
                    {
                        "name": "transform_data",
                        "type": "data_transformation",
                        "config": {
                            "pipeline_id": "customer_enrichment",
                            "input_key": "raw_customers",
                            "output_key": "enriched_customers"
                        }
                    }
                ]
            }
        }
    """
    
    component_type = "data_transformation"
    
    def __init__(
        self,
        name: str,
        config: Dict[str, Any],
        engine: Optional[TransformationEngine] = None,
    ) -> None:
        """
        Initialize the transformation component.
        
        Args:
            name: Component name
            config: Component configuration
            engine: Optional pre-configured TransformationEngine
        """
        self.name = name
        self.config = config
        self._engine = engine
        
        # Extract config options
        self._pipeline_id = config.get("pipeline_id")
        self._input_key = config.get("input_key", "data")
        self._output_key = config.get("output_key", "data")
        self._datasets_key = config.get("datasets_key", "datasets")
        self._variables_key = config.get("variables_key", "variables")
        self._fail_on_error = config.get("fail_on_error", True)
        
        if self._pipeline_id is None:
            raise ValueError(
                f"DataTransformationComponent '{name}' requires 'pipeline_id' in config"
            )
    
    def set_engine(self, engine: TransformationEngine) -> None:
        """Set the transformation engine."""
        self._engine = engine
    
    def execute(self, context: PipelineContext) -> PipelineResult:
        """
        Execute the transformation pipeline.
        
        Args:
            context: Service Pipeline context
            
        Returns:
            PipelineResult indicating success/failure
        """
        if self._engine is None:
            return PipelineResult(
                success=False,
                component_name=self.name,
                error_message="TransformationEngine not configured",
            )
        
        # Get input data from context
        input_data = context.data.get(self._input_key)
        if input_data is None:
            return PipelineResult(
                success=False,
                component_name=self.name,
                error_message=f"Input data not found at key '{self._input_key}'",
            )
        
        # Convert to Polars DataFrame if needed
        if not isinstance(input_data, pl.DataFrame):
            try:
                input_data = pl.DataFrame(input_data)
            except Exception as e:
                return PipelineResult(
                    success=False,
                    component_name=self.name,
                    error_message=f"Failed to convert input to DataFrame: {e}",
                )
        
        # Get additional datasets
        datasets = context.data.get(self._datasets_key, {})
        
        # Get variables
        variables = context.data.get(self._variables_key, {})
        
        # Execute transformation
        try:
            result: TransformationResult = self._engine.transform(
                pipeline_id=self._pipeline_id,
                data=input_data,
                datasets=datasets,
                variables=variables,
                metadata={"component_name": self.name},
            )
        except Exception as e:
            return PipelineResult(
                success=False,
                component_name=self.name,
                error_message=f"Transformation failed: {e}",
            )
        
        # Handle result
        if result.success:
            # Store transformed data in context
            context.data[self._output_key] = result.data
            
            # Store transformation metrics
            context.data[f"{self.name}_metrics"] = {
                "rows_in": result.rows_in,
                "rows_out": result.rows_out,
                "steps_completed": result.steps_completed,
                "total_steps": result.total_steps,
                "execution_time_ms": result.total_execution_time_ms,
            }
            
            return PipelineResult(
                success=True,
                component_name=self.name,
                data={"rows_transformed": result.rows_out},
            )
        else:
            if self._fail_on_error:
                return PipelineResult(
                    success=False,
                    component_name=self.name,
                    error_message=result.error_message,
                )
            else:
                # Log error but continue
                context.data[f"{self.name}_error"] = result.error_message
                return PipelineResult(
                    success=True,
                    component_name=self.name,
                    data={"error": result.error_message},
                )
```

### 7.2 Component Registration

```python
# In frameworks/data_transformation/__init__.py

from frameworks.data_transformation.adapters.pipeline_adapter import (
    DataTransformationComponent,
)
from frameworks.data_transformation.engine.transformation_engine import (
    TransformationEngine,
)
from frameworks.data_transformation.engine.pipeline_builder import Pipeline
from frameworks.data_transformation.contract.transformer import Transformer
from frameworks.data_transformation.contract.result import (
    StepResult,
    TransformationResult,
)


def register_with_service_pipeline(
    component_registry,
    engine: TransformationEngine,
) -> None:
    """
    Register the DataTransformationComponent with Service Pipeline.
    
    Args:
        component_registry: Service Pipeline component registry
        engine: Configured TransformationEngine instance
    """
    def component_factory(name: str, config: dict):
        component = DataTransformationComponent(name, config, engine)
        return component
    
    component_registry.register(
        "data_transformation",
        component_factory,
    )


__all__ = [
    "TransformationEngine",
    "Pipeline",
    "Transformer",
    "StepResult",
    "TransformationResult",
    "DataTransformationComponent",
    "register_with_service_pipeline",
]
```

## 8. Error Handling

### 8.1 Exception Classes

```python
# frameworks/data_transformation/exceptions.py

class TransformationError(Exception):
    """
    Base exception for transformation errors.
    
    Raised when a transformation fails during execution.
    """
    
    def __init__(
        self,
        message: str,
        step_name: str = None,
        transformer_type: str = None,
        cause: Exception = None,
    ):
        self.step_name = step_name
        self.transformer_type = transformer_type
        self.cause = cause
        
        full_message = message
        if step_name:
            full_message = f"[{step_name}] {full_message}"
        if transformer_type:
            full_message = f"({transformer_type}) {full_message}"
        
        super().__init__(full_message)


class ConfigurationError(TransformationError):
    """
    Exception for configuration errors.
    
    Raised when a transformer receives invalid configuration.
    """
    pass


class ExpressionParseError(TransformationError):
    """
    Exception for expression parsing errors.
    
    Raised when an expression string cannot be parsed into a Polars expression.
    """
    
    def __init__(
        self,
        message: str,
        expression: str,
        position: int = None,
    ):
        self.expression = expression
        self.position = position
        
        full_message = f"{message}\nExpression: {expression}"
        if position is not None:
            full_message += f"\nPosition: {position}"
        
        super().__init__(full_message)


class DatasetNotFoundError(TransformationError):
    """
    Exception when a required dataset is not found in context.
    """
    
    def __init__(self, dataset_name: str, step_name: str = None):
        self.dataset_name = dataset_name
        super().__init__(
            f"Dataset '{dataset_name}' not found in context",
            step_name=step_name,
        )


class PipelineNotFoundError(TransformationError):
    """
    Exception when a requested pipeline ID is not found.
    """
    
    def __init__(self, pipeline_id: str):
        self.pipeline_id = pipeline_id
        super().__init__(f"Pipeline '{pipeline_id}' not found")
```

### 8.2 Error Handling Strategy

The framework uses a layered error handling approach:

1. **Configuration Validation**: Errors are caught early during config loading
2. **Pre-execution Validation**: Each transformer validates its config before transform()
3. **Execution Errors**: Caught and wrapped in TransformationError with context
4. **Result Reporting**: Errors are recorded in StepResult and TransformationResult

```python
# Error handling in TransformationEngine.transform()

def transform(
    self,
    pipeline_id: str,
    data: pl.DataFrame,
    datasets: Optional[Dict[str, pl.DataFrame]] = None,
    variables: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> TransformationResult:
    """Execute a transformation pipeline with comprehensive error handling."""
    
    start_time = time.time()
    step_results: List[StepResult] = []
    rows_in = len(data)
    current_data = data
    
    # Validate pipeline exists
    if pipeline_id not in self._pipelines:
        raise PipelineNotFoundError(pipeline_id)
    
    pipeline_config = self._pipelines[pipeline_id]
    
    # Create context
    context = TransformationContext(
        data=current_data,
        datasets=datasets or {},
        variables=variables or {},
        metadata=metadata or {},
        pipeline_id=pipeline_id,
    )
    
    # Execute each step
    for step_config in pipeline_config["steps"]:
        step_name = step_config["name"]
        transformer_type = step_config["type"]
        config = step_config.get("config", {})
        
        step_start = time.time()
        rows_before = len(current_data)
        cols_before = len(current_data.columns)
        
        try:
            # Get transformer class
            transformer_class = self._transformer_registry.get(transformer_type)
            if transformer_class is None:
                raise ConfigurationError(
                    f"Unknown transformer type: {transformer_type}",
                    step_name=step_name,
                )
            
            # Instantiate transformer
            transformer = transformer_class(step_name, config)
            
            # Validate config
            validation_error = transformer.validate_config(config)
            if validation_error:
                raise ConfigurationError(
                    validation_error,
                    step_name=step_name,
                    transformer_type=transformer_type,
                )
            
            # Check required datasets
            required_datasets = transformer.get_required_datasets()
            for ds_name in required_datasets:
                if ds_name and ds_name not in context.datasets:
                    raise DatasetNotFoundError(ds_name, step_name)
            
            # Execute transform
            context.current_step = step_name
            current_data = transformer.transform(current_data, context)
            context.data = current_data
            
            # Record success
            step_result = StepResult(
                step_name=step_name,
                transformer_type=transformer_type,
                success=True,
                rows_in=rows_before,
                rows_out=len(current_data),
                columns_in=cols_before,
                columns_out=len(current_data.columns),
                execution_time_ms=(time.time() - step_start) * 1000,
            )
            
        except Exception as e:
            # Record failure
            step_result = StepResult(
                step_name=step_name,
                transformer_type=transformer_type,
                success=False,
                rows_in=rows_before,
                rows_out=rows_before,
                columns_in=cols_before,
                columns_out=cols_before,
                execution_time_ms=(time.time() - step_start) * 1000,
                error_message=str(e),
            )
            step_results.append(step_result)
            
            # Return failure result
            return TransformationResult(
                pipeline_id=pipeline_id,
                success=False,
                data=None,
                step_results=step_results,
                total_execution_time_ms=(time.time() - start_time) * 1000,
                rows_in=rows_in,
                rows_out=rows_before,
                error_message=str(e),
            )
        
        step_results.append(step_result)
    
    # Return success result
    return TransformationResult(
        pipeline_id=pipeline_id,
        success=True,
        data=current_data,
        step_results=step_results,
        total_execution_time_ms=(time.time() - start_time) * 1000,
        rows_in=rows_in,
        rows_out=len(current_data),
    )
```

## 9. Testing Strategy

### 9.1 Test Structure

```
frameworks/data_transformation/tests/
├── __init__.py
├── conftest.py                           # Shared fixtures
├── unit/
│   ├── __init__.py
│   ├── transformers/
│   │   ├── __init__.py
│   │   ├── column/
│   │   │   ├── test_select_transformer.py
│   │   │   ├── test_drop_transformer.py
│   │   │   ├── test_rename_transformer.py
│   │   │   ├── test_cast_transformer.py
│   │   │   └── test_with_columns_transformer.py
│   │   ├── row/
│   │   │   ├── test_filter_transformer.py
│   │   │   ├── test_sort_transformer.py
│   │   │   ├── test_unique_transformer.py
│   │   │   ├── test_head_transformer.py
│   │   │   ├── test_tail_transformer.py
│   │   │   ├── test_slice_transformer.py
│   │   │   ├── test_sample_transformer.py
│   │   │   └── test_drop_nulls_transformer.py
│   │   ├── reshape/
│   │   │   ├── test_pivot_transformer.py
│   │   │   ├── test_unpivot_transformer.py
│   │   │   └── test_explode_transformer.py
│   │   ├── aggregate/
│   │   │   └── test_group_by_transformer.py
│   │   ├── combine/
│   │   │   ├── test_join_transformer.py
│   │   │   ├── test_concat_transformer.py
│   │   │   └── test_union_transformer.py
│   │   └── fill/
│   │       ├── test_fill_null_transformer.py
│   │       └── test_fill_nan_transformer.py
│   ├── test_transformer_registry.py
│   ├── test_expression_parser.py
│   ├── test_pipeline_builder.py
│   ├── test_transformation_engine.py
│   └── test_config_loader.py
└── integration/
    ├── __init__.py
    ├── test_service_pipeline_integration.py
    └── test_end_to_end_pipelines.py
```

### 9.2 Test Fixtures (conftest.py)

```python
# frameworks/data_transformation/tests/conftest.py

import pytest
import polars as pl

from frameworks.data_transformation.engine.transformation_engine import (
    TransformationEngine,
)
from frameworks.data_transformation.engine.transformation_context import (
    TransformationContext,
)


@pytest.fixture
def sample_customers_df() -> pl.DataFrame:
    """Sample customer DataFrame for testing."""
    return pl.DataFrame({
        "customer_id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "email": ["alice@test.com", "bob@test.com", "charlie@test.com", 
                  "diana@test.com", "eve@test.com"],
        "status": ["active", "active", "inactive", "active", "inactive"],
        "age": [25, 30, 35, 28, 42],
        "signup_date": ["2023-01-15", "2023-02-20", "2023-03-10", 
                        "2023-04-05", "2023-05-12"],
    })


@pytest.fixture
def sample_orders_df() -> pl.DataFrame:
    """Sample orders DataFrame for testing joins."""
    return pl.DataFrame({
        "order_id": [101, 102, 103, 104, 105, 106],
        "customer_id": [1, 1, 2, 3, 1, 4],
        "amount": [100.0, 150.0, 200.0, 75.0, 300.0, 125.0],
        "order_date": ["2023-06-01", "2023-06-15", "2023-06-10",
                       "2023-06-20", "2023-07-01", "2023-07-05"],
        "status": ["completed", "completed", "completed", 
                   "cancelled", "pending", "completed"],
    })


@pytest.fixture
def sample_products_df() -> pl.DataFrame:
    """Sample products DataFrame for testing."""
    return pl.DataFrame({
        "product_id": [1, 2, 3, 4],
        "name": ["Widget", "Gadget", "Gizmo", "Doohickey"],
        "price": [10.99, 24.99, 5.49, 15.00],
        "category": ["electronics", "electronics", "accessories", "tools"],
    })


@pytest.fixture
def df_with_nulls() -> pl.DataFrame:
    """DataFrame with null values for testing null handling."""
    return pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", None, "Charlie", "Diana", None],
        "value": [100.0, 200.0, None, 400.0, 500.0],
        "category": ["A", "B", None, "A", "B"],
    })


@pytest.fixture
def df_with_lists() -> pl.DataFrame:
    """DataFrame with list columns for testing explode."""
    return pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "tags": [["python", "data"], ["java", "web"], ["python", "ml", "ai"]],
    })


@pytest.fixture
def empty_context(sample_customers_df) -> TransformationContext:
    """Empty transformation context for testing."""
    return TransformationContext(data=sample_customers_df)


@pytest.fixture
def context_with_datasets(
    sample_customers_df,
    sample_orders_df,
    sample_products_df,
) -> TransformationContext:
    """Context with additional datasets for testing joins."""
    return TransformationContext(
        data=sample_customers_df,
        datasets={
            "orders": sample_orders_df,
            "products": sample_products_df,
        },
    )


@pytest.fixture
def transformation_engine() -> TransformationEngine:
    """Fresh transformation engine instance."""
    return TransformationEngine()
```

### 9.3 Example Unit Test

```python
# frameworks/data_transformation/tests/unit/transformers/column/test_select_transformer.py

import pytest
import polars as pl

from frameworks.data_transformation.transformers.column.select import (
    SelectTransformer,
)
from frameworks.data_transformation.engine.transformation_context import (
    TransformationContext,
)
from frameworks.data_transformation.exceptions import ConfigurationError


class TestSelectTransformer:
    """Tests for SelectTransformer."""
    
    def test_select_single_column(self, sample_customers_df, empty_context):
        """Test selecting a single column."""
        transformer = SelectTransformer(
            name="select_name",
            config={"columns": ["name"]}
        )
        
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert result.columns == ["name"]
        assert len(result) == len(sample_customers_df)
    
    def test_select_multiple_columns(self, sample_customers_df, empty_context):
        """Test selecting multiple columns."""
        transformer = SelectTransformer(
            name="select_cols",
            config={"columns": ["customer_id", "name", "email"]}
        )
        
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert result.columns == ["customer_id", "name", "email"]
        assert len(result) == len(sample_customers_df)
    
    def test_select_reorders_columns(self, sample_customers_df, empty_context):
        """Test that select reorders columns as specified."""
        transformer = SelectTransformer(
            name="select_reorder",
            config={"columns": ["email", "name", "customer_id"]}
        )
        
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert result.columns == ["email", "name", "customer_id"]
    
    def test_select_missing_column_raises_error(
        self, sample_customers_df, empty_context
    ):
        """Test that selecting non-existent column raises error."""
        transformer = SelectTransformer(
            name="select_missing",
            config={"columns": ["nonexistent"]}
        )
        
        with pytest.raises(Exception):  # Polars will raise
            transformer.transform(sample_customers_df, empty_context)
    
    def test_validate_config_missing_columns(self):
        """Test config validation catches missing columns."""
        transformer = SelectTransformer(name="test", config={})
        
        error = transformer.validate_config({})
        
        assert error is not None
        assert "columns" in error.lower()
    
    def test_validate_config_empty_columns(self):
        """Test config validation catches empty columns list."""
        transformer = SelectTransformer(name="test", config={"columns": []})
        
        error = transformer.validate_config({"columns": []})
        
        assert error is not None
        assert "empty" in error.lower()
    
    def test_validate_config_columns_not_list(self):
        """Test config validation catches non-list columns."""
        transformer = SelectTransformer(
            name="test", 
            config={"columns": "name"}
        )
        
        error = transformer.validate_config({"columns": "name"})
        
        assert error is not None
        assert "list" in error.lower()
    
    def test_transformer_type(self):
        """Test transformer_type property."""
        transformer = SelectTransformer(
            name="test",
            config={"columns": ["name"]}
        )
        
        assert transformer.transformer_type == "select"
```

### 9.4 Example Integration Test

```python
# frameworks/data_transformation/tests/integration/test_service_pipeline_integration.py

import pytest
import polars as pl

from frameworks.data_transformation.engine.transformation_engine import (
    TransformationEngine,
)
from frameworks.data_transformation.adapters.pipeline_adapter import (
    DataTransformationComponent,
)


class TestServicePipelineIntegration:
    """Integration tests for Service Pipeline adapter."""
    
    @pytest.fixture
    def engine_with_pipeline(self, sample_orders_df):
        """Engine configured with a test pipeline."""
        engine = TransformationEngine()
        
        engine.add_pipeline(
            "test_pipeline",
            {
                "description": "Test pipeline",
                "steps": [
                    {
                        "name": "filter_completed",
                        "type": "filter",
                        "config": {"condition": "col('status') == 'completed'"}
                    },
                    {
                        "name": "select_columns",
                        "type": "select",
                        "config": {"columns": ["order_id", "customer_id", "amount"]}
                    },
                ],
            }
        )
        
        return engine
    
    def test_component_transforms_data(
        self, engine_with_pipeline, sample_orders_df
    ):
        """Test component successfully transforms data."""
        component = DataTransformationComponent(
            name="transform",
            config={
                "pipeline_id": "test_pipeline",
                "input_key": "orders",
                "output_key": "filtered_orders",
            },
            engine=engine_with_pipeline,
        )
        
        # Simulate pipeline context
        class MockContext:
            data = {"orders": sample_orders_df}
        
        context = MockContext()
        result = component.execute(context)
        
        assert result.success
        assert "filtered_orders" in context.data
        
        output_df = context.data["filtered_orders"]
        assert len(output_df.columns) == 3
        assert all(
            status == "completed" 
            for status in sample_orders_df.filter(
                pl.col("order_id").is_in(output_df["order_id"])
            )["status"]
        )
    
    def test_component_handles_missing_input(self, engine_with_pipeline):
        """Test component handles missing input data gracefully."""
        component = DataTransformationComponent(
            name="transform",
            config={
                "pipeline_id": "test_pipeline",
                "input_key": "missing_key",
            },
            engine=engine_with_pipeline,
        )
        
        class MockContext:
            data = {}
        
        result = component.execute(MockContext())
        
        assert not result.success
        assert "not found" in result.error_message.lower()
    
    def test_component_stores_metrics(
        self, engine_with_pipeline, sample_orders_df
    ):
        """Test component stores transformation metrics."""
        component = DataTransformationComponent(
            name="my_transform",
            config={
                "pipeline_id": "test_pipeline",
                "input_key": "data",
            },
            engine=engine_with_pipeline,
        )
        
        class MockContext:
            data = {"data": sample_orders_df}
        
        context = MockContext()
        result = component.execute(context)
        
        assert result.success
        assert "my_transform_metrics" in context.data
        
        metrics = context.data["my_transform_metrics"]
        assert "rows_in" in metrics
        assert "rows_out" in metrics
        assert "execution_time_ms" in metrics
```

## 10. Usage Examples

### 10.1 Fluent API Example

```python
import polars as pl
from frameworks.data_transformation import Pipeline

# Create sample data
customers = pl.DataFrame({
    "customer_id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
    "email": ["alice@test.com", "bob@test.com", "charlie@test.com",
              "diana@test.com", "eve@test.com"],
    "status": ["active", "active", "inactive", "active", "inactive"],
})

orders = pl.DataFrame({
    "order_id": [101, 102, 103, 104, 105],
    "customer_id": [1, 1, 2, 3, 4],
    "amount": [100.0, 150.0, 200.0, 75.0, 300.0],
})

# Build and execute pipeline using fluent API
pipeline = (
    Pipeline("customer_summary")
    .filter(pl.col("status") == "active")
    .join(right="orders", on="customer_id", how="left")
    .group_by(
        by=["customer_id", "name", "email"],
        agg={
            "total_orders": {"column": "order_id", "agg": "count"},
            "total_spent": {"column": "amount", "agg": "sum"},
        }
    )
    .with_columns(
        tier="when(col('total_spent') >= 200).then('gold').otherwise('silver')"
    )
    .select(["customer_id", "name", "total_orders", "total_spent", "tier"])
    .sort(by="total_spent", descending=True)
)

result = pipeline.execute(
    data=customers,
    datasets={"orders": orders}
)

print(f"Success: {result.success}")
print(f"Rows: {result.rows_in} -> {result.rows_out}")
print(f"Time: {result.total_execution_time_ms:.2f}ms")
print(result.data)
```

### 10.2 Configuration-Driven Example

```python
from frameworks.data_transformation import TransformationEngine

# Initialize engine with config file
engine = TransformationEngine(
    pipeline_config_path="config/transformations.yaml"
)

# Load data
customers = pl.read_csv("data/customers.csv")
orders = pl.read_csv("data/orders.csv")

# Execute pipeline by ID
result = engine.transform(
    pipeline_id="customer_enrichment",
    data=customers,
    datasets={"orders": orders},
    variables={"min_spend": 100},
)

if result.success:
    # Save transformed data
    result.data.write_csv("output/enriched_customers.csv")
    
    # Print step-by-step summary
    for step in result.step_results:
        print(f"{step.step_name}: {step.rows_in} -> {step.rows_out} "
              f"({step.execution_time_ms:.1f}ms)")
else:
    print(f"Pipeline failed: {result.error_message}")
```

### 10.3 Service Pipeline Integration Example

```python
from frameworks.service_pipeline import ServicePipeline, ComponentRegistry
from frameworks.data_transformation import (
    TransformationEngine,
    register_with_service_pipeline,
)

# Setup transformation engine
transformation_engine = TransformationEngine(
    pipeline_config_path="config/transformations.yaml"
)

# Register with Service Pipeline
component_registry = ComponentRegistry()
register_with_service_pipeline(component_registry, transformation_engine)

# Define Service Pipeline with transformation component
service_pipeline = ServicePipeline(
    config={
        "pipeline": {
            "name": "data_processing_pipeline",
            "components": [
                {
                    "name": "load_data",
                    "type": "data_loader",
                    "config": {"source": "s3://bucket/customers.parquet"}
                },
                {
                    "name": "transform_customers",
                    "type": "data_transformation",
                    "config": {
                        "pipeline_id": "customer_enrichment",
                        "input_key": "raw_data",
                        "output_key": "transformed_data",
                    }
                },
                {
                    "name": "validate_output",
                    "type": "data_validation",
                    "config": {
                        "schema_id": "enriched_customer_schema",
                        "input_key": "transformed_data",
                    }
                },
                {
                    "name": "save_data",
                    "type": "data_writer",
                    "config": {
                        "destination": "s3://bucket/enriched_customers.parquet",
                        "input_key": "transformed_data",
                    }
                },
            ]
        }
    },
    component_registry=component_registry,
)

# Execute the full pipeline
result = service_pipeline.execute(
    datasets={"orders": orders_df}
)
```

### 10.4 Custom Transformer Example

```python
from frameworks.data_transformation import Transformer, TransformationEngine
from frameworks.data_transformation.transformers.base_transformer import (
    BaseTransformer,
)

import polars as pl


class NormalizeTransformer(BaseTransformer):
    """
    Custom transformer to normalize numeric columns to 0-1 range.
    
    Configuration:
        columns: List of columns to normalize
        method: "minmax" or "zscore" (default: "minmax")
    """
    
    transformer_type = "normalize"
    
    def transform(
        self,
        data: pl.DataFrame,
        context,
    ) -> pl.DataFrame:
        columns = self._get_required("columns", list)
        method = self._get_optional("method", "minmax", str)
        
        exprs = []
        for col in columns:
            if method == "minmax":
                # (x - min) / (max - min)
                expr = (
                    (pl.col(col) - pl.col(col).min()) /
                    (pl.col(col).max() - pl.col(col).min())
                ).alias(col)
            elif method == "zscore":
                # (x - mean) / std
                expr = (
                    (pl.col(col) - pl.col(col).mean()) /
                    pl.col(col).std()
                ).alias(col)
            else:
                raise ValueError(f"Unknown normalization method: {method}")
            
            exprs.append(expr)
        
        return data.with_columns(exprs)


# Register custom transformer
engine = TransformationEngine()
engine.register_transformer("normalize", NormalizeTransformer)

# Use in pipeline
pipeline_config = {
    "steps": [
        {
            "name": "normalize_features",
            "type": "normalize",
            "config": {
                "columns": ["age", "income", "score"],
                "method": "minmax"
            }
        }
    ]
}
```

### 10.5 Expression Examples

The expression parser supports a safe subset of Polars expressions for use in configuration files:

```python
# Simple column references
"col('name')"
"col('price') * col('quantity')"

# Comparisons
"col('age') >= 18"
"col('status') == 'active'"
"col('amount') > 100"

# Boolean logic
"(col('age') >= 18) & (col('status') == 'active')"
"col('is_premium') | (col('total_spent') > 1000)"
"~col('is_blocked')"

# Null handling
"col('value').is_null()"
"col('name').is_not_null()"
"col('amount').fill_null(0)"

# String operations
"col('name').str.to_lowercase()"
"col('email').str.contains('@company.com')"
"col('code').str.starts_with('PRE')"

# Numeric operations
"col('price').round(2)"
"col('value').abs()"
"(col('high') + col('low')) / 2"

# Date operations
"col('date').dt.year()"
"col('timestamp').dt.strftime('%Y-%m-%d')"

# Conditionals
"when(col('age') >= 18).then('adult').otherwise('minor')"
"when(col('score') >= 90).then('A').when(col('score') >= 80).then('B').otherwise('C')"

# Aggregations (in group_by context)
"col('amount').sum()"
"col('price').mean()"
"col('order_id').count()"
"col('date').min()"
```

---

## Appendix: Transformer Quick Reference

| Category | Transformer | Description |
|----------|-------------|-------------|
| **Column** | `select` | Select specific columns |
| | `drop` | Remove columns |
| | `rename` | Rename columns |
| | `cast` | Change column data types |
| | `with_columns` | Add/modify columns with expressions |
| **Row** | `filter` | Filter rows by condition |
| | `sort` | Sort rows |
| | `unique` | Remove duplicates |
| | `head` | Take first N rows |
| | `tail` | Take last N rows |
| | `slice` | Take rows from offset |
| | `sample` | Random sample of rows |
| | `drop_nulls` | Remove rows with nulls |
| **Reshape** | `pivot` | Wide format (rows to columns) |
| | `unpivot` | Long format (columns to rows) |
| | `explode` | Expand list columns |
| **Aggregate** | `group_by` | Group and aggregate |
| **Combine** | `join` | Join with another dataset |
| | `concat` | Concatenate datasets |
| | `union` | Union (concat + unique) |
| **Fill** | `fill_null` | Fill null values |
| | `fill_nan` | Fill NaN values |
