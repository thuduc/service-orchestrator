"""Schema validation stage using Pandera for Polars."""

import time
from typing import Any, Dict, List, Optional

import pandera.polars as pa
import polars as pl
from pandera.errors import SchemaErrors

from frameworks.data_validation.engine.validation_context import ValidationContext
from frameworks.data_validation.engine.validation_result import (
    StageResult,
    ValidationError,
    ValidationWarning,
)
from frameworks.data_validation.stages.base_stage import BaseValidationStage


# Mapping from config dtype strings to Polars dtypes
DTYPE_MAP = {
    "Int8": pl.Int8,
    "Int16": pl.Int16,
    "Int32": pl.Int32,
    "Int64": pl.Int64,
    "UInt8": pl.UInt8,
    "UInt16": pl.UInt16,
    "UInt32": pl.UInt32,
    "UInt64": pl.UInt64,
    "Float32": pl.Float32,
    "Float64": pl.Float64,
    "Boolean": pl.Boolean,
    "Utf8": pl.Utf8,
    "String": pl.String,
    "Date": pl.Date,
    "Datetime": pl.Datetime,
    "Time": pl.Time,
    "Duration": pl.Duration,
    "Categorical": pl.Categorical,
    "Null": pl.Null,
    "Object": pl.Object,
}


class SchemaValidationStage(BaseValidationStage):
    """
    Validates DataFrame schema using Pandera for Polars.
    
    This stage validates:
    - Column data types
    - Nullable constraints
    - Unique constraints
    - Built-in Pandera checks (greater_than, isin, str_matches, etc.)
    
    Configuration options:
    - coerce: If True, attempt to coerce values to the specified dtypes
    - strict: If True, fail if DataFrame has columns not in schema
    - drop_invalid_rows: If True, remove failing rows instead of failing
    - treat_dropped_as_failure: If True, stage is_valid=false even when rows are dropped
    """

    stage_type = "schema_validation"

    def __init__(self, name: str, config: Dict[str, Any]) -> None:
        """
        Initialize the schema validation stage.
        
        Args:
            name: Instance name for this stage
            config: Stage configuration dictionary with columns and options
        """
        super().__init__(name, config)
        self._schema = self._build_pandera_schema(config)
        self._coerce = config.get("coerce", False)
        self._drop_invalid_rows = config.get("drop_invalid_rows", False)
        self._treat_dropped_as_failure = config.get("treat_dropped_as_failure", False)

    def _map_dtype(self, dtype_str: Optional[str]) -> Optional[type]:
        """
        Map dtype string to Polars dtype.
        
        Args:
            dtype_str: String representation of dtype
            
        Returns:
            Polars dtype class or None if not specified
        """
        if dtype_str is None:
            return None
        
        if dtype_str not in DTYPE_MAP:
            raise ValueError(
                f"Unknown dtype '{dtype_str}'. "
                f"Valid dtypes: {', '.join(DTYPE_MAP.keys())}"
            )
        
        return DTYPE_MAP[dtype_str]

    def _build_checks(self, check_configs: List[Dict[str, Any]]) -> List[pa.Check]:
        """
        Build Pandera Check objects from configuration.
        
        Args:
            check_configs: List of check configuration dictionaries
            
        Returns:
            List of Pandera Check objects
        """
        checks = []
        
        for check_config in check_configs:
            if "builtin" not in check_config:
                continue
            
            check_name = check_config["builtin"]
            params = {k: v for k, v in check_config.items() if k != "builtin"}
            
            # Get the check method from pa.Check
            if not hasattr(pa.Check, check_name):
                raise ValueError(f"Unknown Pandera check: {check_name}")
            
            check_method = getattr(pa.Check, check_name)
            checks.append(check_method(**params))
        
        return checks

    def _build_pandera_schema(self, config: Dict[str, Any]) -> pa.DataFrameSchema:
        """
        Build a Pandera Polars schema from configuration.
        
        Args:
            config: Stage configuration dictionary
            
        Returns:
            Pandera DataFrameSchema for Polars
        """
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

    def _parse_pandera_errors(
        self, 
        exc: SchemaErrors,
        data: pl.DataFrame
    ) -> List[ValidationError]:
        """
        Parse Pandera SchemaErrors into ValidationError objects.
        
        Args:
            exc: Pandera SchemaErrors exception
            data: The original DataFrame being validated
            
        Returns:
            List of ValidationError objects
        """
        errors = []
        
        # Access the failure cases DataFrame from the exception
        try:
            failure_cases = exc.failure_cases
            
            if failure_cases is not None and len(failure_cases) > 0:
                # Group errors by check and column
                grouped = {}
                
                for row in failure_cases.iter_rows(named=True):
                    check_name = row.get("check", "unknown")
                    column = row.get("column", None)
                    failure_case = row.get("failure_case", None)
                    index = row.get("index", None)
                    
                    key = (check_name, column)
                    if key not in grouped:
                        grouped[key] = {
                            "failure_cases": [],
                            "row_indices": [],
                        }
                    
                    if failure_case is not None:
                        grouped[key]["failure_cases"].append(failure_case)
                    if index is not None:
                        grouped[key]["row_indices"].append(int(index))
                
                for (check_name, column), details in grouped.items():
                    errors.append(ValidationError(
                        check_name=str(check_name),
                        column=column,
                        error_message=f"Check '{check_name}' failed for column '{column}'",
                        failure_cases=details["failure_cases"][:10] if details["failure_cases"] else None,
                        row_indices=details["row_indices"] if details["row_indices"] else None,
                    ))
            else:
                # Fallback: create generic error from exception message
                errors.append(ValidationError(
                    check_name="schema_validation",
                    column=None,
                    error_message=str(exc),
                    failure_cases=None,
                    row_indices=None,
                ))
        except Exception:
            # Fallback for any parsing issues
            errors.append(ValidationError(
                check_name="schema_validation",
                column=None,
                error_message=str(exc),
                failure_cases=None,
                row_indices=None,
            ))
        
        return errors

    def execute(self, context: ValidationContext) -> StageResult:
        """
        Execute schema validation with proper data propagation.
        
        This method:
        1. Validates the DataFrame against the schema
        2. Handles coercion if enabled
        3. Handles drop_invalid_rows if enabled
        4. Returns appropriate StageResult with output_data for data-modifying operations
        
        Args:
            context: Validation context containing the data
            
        Returns:
            StageResult with validation outcome
        """
        start = time.perf_counter()
        
        try:
            # Validate and potentially coerce the data
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
                output_data=validated_df if self._coerce else None,
            )
        except SchemaErrors as exc:
            errors = self._parse_pandera_errors(exc, context.data)
            execution_ms = (time.perf_counter() - start) * 1000
            
            if self._drop_invalid_rows:
                # Collect all failing row indices
                failing_indices = set()
                for error in errors:
                    if error.row_indices:
                        failing_indices.update(error.row_indices)
                
                # Remove failing rows from data
                all_indices = set(range(len(context.data)))
                valid_indices = sorted(all_indices - failing_indices)
                cleaned_df = context.data[valid_indices]
                
                if self._treat_dropped_as_failure:
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
                # Standard failure without row dropping
                failing_indices = set()
                for e in errors:
                    if e.row_indices:
                        failing_indices.update(e.row_indices)
                
                return StageResult(
                    stage_name=self.name,
                    stage_type=self.stage_type,
                    is_valid=False,
                    errors=errors,
                    warnings=[],
                    execution_time_ms=execution_ms,
                    rows_validated=len(context.data),
                    rows_failed=len(failing_indices),
                    output_data=None,
                )

    def validate_config(self, config: Dict[str, Any]) -> Optional[str]:
        """
        Validate the schema validation stage configuration.
        
        Args:
            config: Stage configuration dictionary
            
        Returns:
            None if valid, error message if invalid
        """
        if "columns" not in config:
            return "Schema validation stage requires 'columns' configuration"
        
        if not config["columns"]:
            return "Schema validation stage requires at least one column definition"
        
        for col_name, col_config in config["columns"].items():
            dtype = col_config.get("dtype")
            if dtype and dtype not in DTYPE_MAP:
                return f"Unknown dtype '{dtype}' for column '{col_name}'"
        
        return None
