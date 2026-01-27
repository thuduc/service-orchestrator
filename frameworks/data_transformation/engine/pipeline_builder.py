"""Pipeline - Fluent builder for constructing transformation pipelines."""

from typing import Any, Dict, List, Optional, Union

import polars as pl

from frameworks.data_transformation.contract.result import TransformationResult


class Pipeline:
    """
    Fluent builder for constructing transformation pipelines.
    
    Provides a chainable API for building pipelines programmatically,
    with full IDE autocomplete support.
    
    Example:
        pipeline = (
            Pipeline("my_pipeline")
            .filter("col('status') == 'active'")
            .join(right="orders", on="customer_id", how="left")
            .group_by(by=["customer_id"], agg={"total": "col('amount').sum()"})
            .select(["customer_id", "total"])
        )
        
        result = pipeline.execute(data, datasets={"orders": orders_df})
    """
    
    def __init__(self, pipeline_id: str, description: str = "") -> None:
        """
        Initialize pipeline builder.
        
        Args:
            pipeline_id: Unique identifier for the pipeline
            description: Optional description of the pipeline's purpose
        """
        self._pipeline_id = pipeline_id
        self._description = description
        self._steps: List[Dict[str, Any]] = []
        self._engine: Optional["TransformationEngine"] = None
    
    # Column operations
    def select(self, columns: List[str]) -> "Pipeline":
        """
        Select specific columns.
        
        Args:
            columns: List of column names to select
            
        Returns:
            Self for method chaining
        """
        self._steps.append({
            "name": f"select_{len(self._steps)}",
            "type": "select",
            "config": {"columns": columns}
        })
        return self
    
    def drop(self, columns: List[str]) -> "Pipeline":
        """
        Drop specific columns.
        
        Args:
            columns: List of column names to drop
            
        Returns:
            Self for method chaining
        """
        self._steps.append({
            "name": f"drop_{len(self._steps)}",
            "type": "drop",
            "config": {"columns": columns}
        })
        return self
    
    def rename(self, mapping: Dict[str, str]) -> "Pipeline":
        """
        Rename columns.
        
        Args:
            mapping: Dictionary mapping old names to new names
            
        Returns:
            Self for method chaining
        """
        self._steps.append({
            "name": f"rename_{len(self._steps)}",
            "type": "rename",
            "config": {"mapping": mapping}
        })
        return self
    
    def cast(self, schema: Dict[str, str]) -> "Pipeline":
        """
        Cast columns to specified types.
        
        Args:
            schema: Dictionary mapping column names to Polars type names
            
        Returns:
            Self for method chaining
        """
        self._steps.append({
            "name": f"cast_{len(self._steps)}",
            "type": "cast",
            "config": {"schema": schema}
        })
        return self
    
    def with_columns(self, columns: Dict[str, Any]) -> "Pipeline":
        """
        Add or modify columns using expressions.
        
        Args:
            columns: Dictionary mapping column names to expressions
            
        Returns:
            Self for method chaining
            
        Example:
            .with_columns({
                "full_name": "col('first') + ' ' + col('last')",
                "total": "col('price') * col('quantity')"
            })
        """
        self._steps.append({
            "name": f"with_columns_{len(self._steps)}",
            "type": "with_columns",
            "config": {"columns": columns}
        })
        return self
    
    # Row operations
    def filter(self, condition: str) -> "Pipeline":
        """
        Filter rows based on condition.
        
        Args:
            condition: Filter expression as string (e.g., "col('age') > 18")
            
        Returns:
            Self for method chaining
        """
        self._steps.append({
            "name": f"filter_{len(self._steps)}",
            "type": "filter",
            "config": {"condition": condition}
        })
        return self
    
    def sort(
        self, 
        by: Union[str, List[str]], 
        descending: Union[bool, List[bool]] = False,
        nulls_last: bool = False,
    ) -> "Pipeline":
        """
        Sort rows.
        
        Args:
            by: Column name or list of column names to sort by
            descending: Sort order (single bool or list matching 'by')
            nulls_last: If True, nulls appear at the end
            
        Returns:
            Self for method chaining
        """
        self._steps.append({
            "name": f"sort_{len(self._steps)}",
            "type": "sort",
            "config": {"by": by, "descending": descending, "nulls_last": nulls_last}
        })
        return self
    
    def unique(
        self, 
        subset: Optional[List[str]] = None,
        keep: str = "first",
        maintain_order: bool = True,
    ) -> "Pipeline":
        """
        Remove duplicate rows.
        
        Args:
            subset: Columns to consider for uniqueness (default: all)
            keep: Which duplicate to keep: "first", "last", "any", "none"
            maintain_order: Preserve original row order
            
        Returns:
            Self for method chaining
        """
        self._steps.append({
            "name": f"unique_{len(self._steps)}",
            "type": "unique",
            "config": {"subset": subset, "keep": keep, "maintain_order": maintain_order}
        })
        return self
    
    def head(self, n: int = 5) -> "Pipeline":
        """
        Take first n rows.
        
        Args:
            n: Number of rows to take
            
        Returns:
            Self for method chaining
        """
        self._steps.append({
            "name": f"head_{len(self._steps)}",
            "type": "head",
            "config": {"n": n}
        })
        return self
    
    def tail(self, n: int = 5) -> "Pipeline":
        """
        Take last n rows.
        
        Args:
            n: Number of rows to take
            
        Returns:
            Self for method chaining
        """
        self._steps.append({
            "name": f"tail_{len(self._steps)}",
            "type": "tail",
            "config": {"n": n}
        })
        return self
    
    def slice(self, offset: int, length: int) -> "Pipeline":
        """
        Slice rows from offset with given length.
        
        Args:
            offset: Starting row index
            length: Number of rows to take
            
        Returns:
            Self for method chaining
        """
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
        seed: Optional[int] = None,
        with_replacement: bool = False,
    ) -> "Pipeline":
        """
        Sample rows.
        
        Args:
            n: Number of rows to sample (mutually exclusive with fraction)
            fraction: Fraction of rows to sample (mutually exclusive with n)
            seed: Random seed for reproducibility
            with_replacement: Allow sampling same row multiple times
            
        Returns:
            Self for method chaining
        """
        config: Dict[str, Any] = {"with_replacement": with_replacement}
        if n is not None:
            config["n"] = n
        if fraction is not None:
            config["fraction"] = fraction
        if seed is not None:
            config["seed"] = seed
            
        self._steps.append({
            "name": f"sample_{len(self._steps)}",
            "type": "sample",
            "config": config
        })
        return self
    
    def drop_nulls(self, subset: Optional[List[str]] = None) -> "Pipeline":
        """
        Drop rows with null values.
        
        Args:
            subset: Columns to check for nulls (default: all)
            
        Returns:
            Self for method chaining
        """
        config: Dict[str, Any] = {}
        if subset is not None:
            config["subset"] = subset
            
        self._steps.append({
            "name": f"drop_nulls_{len(self._steps)}",
            "type": "drop_nulls",
            "config": config
        })
        return self
    
    # Reshape operations
    def pivot(
        self,
        on: Union[str, List[str]],
        index: Union[str, List[str]],
        values: Union[str, List[str]],
        aggregate_function: str = "first",
    ) -> "Pipeline":
        """
        Pivot table from long to wide format.
        
        Args:
            on: Column(s) whose values become new column names
            index: Column(s) to use as row identifiers
            values: Column(s) containing values to fill the matrix
            aggregate_function: How to aggregate duplicates
            
        Returns:
            Self for method chaining
        """
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
        value_name: str = "value",
    ) -> "Pipeline":
        """
        Unpivot (melt) table from wide to long format.
        
        Args:
            on: Column(s) to unpivot
            index: Column(s) to keep as identifiers
            variable_name: Name for the new variable column
            value_name: Name for the new value column
            
        Returns:
            Self for method chaining
        """
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
        """
        Explode list columns into separate rows.
        
        Args:
            columns: Column name or list of columns to explode
            
        Returns:
            Self for method chaining
        """
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
        agg: Dict[str, Any],
    ) -> "Pipeline":
        """
        Group by and aggregate.
        
        Args:
            by: Column(s) to group by
            agg: Dictionary mapping output column names to aggregation expressions
            
        Returns:
            Self for method chaining
            
        Example:
            .group_by(
                by=["category"],
                agg={
                    "total_sales": "col('amount').sum()",
                    "avg_price": "col('price').mean()",
                    "count": "col('id').count()"
                }
            )
        """
        self._steps.append({
            "name": f"group_by_{len(self._steps)}",
            "type": "group_by",
            "config": {"by": by, "aggregations": agg}
        })
        return self
    
    # Combine operations
    def join(
        self,
        right: str,
        on: Optional[Union[str, List[str]]] = None,
        left_on: Optional[Union[str, List[str]]] = None,
        right_on: Optional[Union[str, List[str]]] = None,
        how: str = "inner",
        suffix: str = "_right",
    ) -> "Pipeline":
        """
        Join with another dataset.
        
        Args:
            right: Name of dataset from context to join with
            on: Column(s) to join on (same name in both)
            left_on: Column(s) from left DataFrame
            right_on: Column(s) from right DataFrame
            how: Join type ("inner", "left", "right", "outer", "semi", "anti", "cross")
            suffix: Suffix for duplicate column names
            
        Returns:
            Self for method chaining
        """
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
        how: str = "vertical",
    ) -> "Pipeline":
        """
        Concatenate with other datasets.
        
        Args:
            datasets: List of dataset names from context
            how: "vertical" (stack rows) or "horizontal" (stack columns)
            
        Returns:
            Self for method chaining
        """
        self._steps.append({
            "name": f"concat_{len(self._steps)}",
            "type": "concat",
            "config": {"datasets": datasets, "how": how}
        })
        return self
    
    def union(self, dataset: str) -> "Pipeline":
        """
        Union with another dataset (vertical stack + unique).
        
        Args:
            dataset: Name of dataset from context
            
        Returns:
            Self for method chaining
        """
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
        columns: Optional[List[str]] = None,
    ) -> "Pipeline":
        """
        Fill null values.
        
        Args:
            value: Literal value to fill nulls with
            strategy: Fill strategy ("forward", "backward", "min", "max", "mean", "zero", "one")
            columns: Columns to fill (default: all)
            
        Returns:
            Self for method chaining
            
        Note:
            Use either 'value' or 'strategy', not both.
        """
        config: Dict[str, Any] = {}
        if value is not None:
            config["value"] = value
        if strategy is not None:
            config["strategy"] = strategy
        if columns is not None:
            config["columns"] = columns
            
        self._steps.append({
            "name": f"fill_null_{len(self._steps)}",
            "type": "fill_null",
            "config": config
        })
        return self
    
    def fill_nan(
        self,
        value: Any,
        columns: Optional[List[str]] = None,
    ) -> "Pipeline":
        """
        Fill NaN values in floating-point columns.
        
        Args:
            value: Value to replace NaN with
            columns: Columns to fill (default: all float columns)
            
        Returns:
            Self for method chaining
        """
        config: Dict[str, Any] = {"value": value}
        if columns is not None:
            config["columns"] = columns
            
        self._steps.append({
            "name": f"fill_nan_{len(self._steps)}",
            "type": "fill_nan",
            "config": config
        })
        return self
    
    # Pipeline execution
    def execute(
        self,
        data: pl.DataFrame,
        datasets: Optional[Dict[str, pl.DataFrame]] = None,
    ) -> TransformationResult:
        """
        Execute the pipeline.
        
        Args:
            data: Primary DataFrame to transform
            datasets: Additional named DataFrames for joins/lookups
            
        Returns:
            TransformationResult with transformed data and execution info
        """
        # Lazy import to avoid circular dependency
        from frameworks.data_transformation.engine.transformation_engine import TransformationEngine
        
        if self._engine is None:
            self._engine = TransformationEngine()
        
        # Add pipeline to engine
        self._engine.add_pipeline(
            self._pipeline_id,
            self.to_config(),
            overwrite=True
        )
        
        return self._engine.transform(
            pipeline_id=self._pipeline_id,
            data=data,
            datasets=datasets,
        )
    
    def to_config(self) -> Dict[str, Any]:
        """
        Export pipeline configuration as dictionary.
        
        Returns:
            Dictionary representation of the pipeline configuration
        """
        return {
            "description": self._description,
            "steps": self._steps,
        }
    
    @property
    def pipeline_id(self) -> str:
        """Get the pipeline ID."""
        return self._pipeline_id
    
    @property
    def steps(self) -> List[Dict[str, Any]]:
        """Get the list of pipeline steps."""
        return self._steps.copy()
    
    def __repr__(self) -> str:
        """String representation of the pipeline."""
        return f"Pipeline(id='{self._pipeline_id}', steps={len(self._steps)})"
