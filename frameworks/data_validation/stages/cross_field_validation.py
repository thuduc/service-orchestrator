"""Cross-field validation stage."""

import time
from typing import Any, Dict, List, Optional

from frameworks.data_validation.contract.check import CheckResult
from frameworks.data_validation.engine.validation_context import ValidationContext
from frameworks.data_validation.engine.validation_result import (
    StageResult,
    ValidationError,
    ValidationWarning,
)
from frameworks.data_validation.registries.check_registry import CheckRegistry
from frameworks.data_validation.stages.base_stage import BaseValidationStage


class CrossFieldValidationStage(BaseValidationStage):
    """
    Validates relationships and constraints across multiple columns within the same row.
    
    This stage is specifically designed for validations that involve multiple columns,
    such as:
    - Date ranges (start_date < end_date)
    - Sum constraints (part1 + part2 = total)
    - Conditional requirements (if status='active', then required_field must be set)
    
    Configuration options:
    - fail_fast: If True, stop after the first rule failure
    - rules: List of cross-field rule configurations
    
    Rule configuration:
    - check_id: ID of the custom check in the registry
    - columns: List of column names involved in the validation (required)
    - params: Dictionary of parameters to pass to the check
    - error_message: Custom error message (overrides check default)
    """

    stage_type = "cross_field_validation"

    def __init__(
        self, 
        name: str, 
        config: Dict[str, Any],
        check_registry: CheckRegistry,
    ) -> None:
        """
        Initialize the cross-field validation stage.
        
        Args:
            name: Instance name for this stage
            config: Stage configuration dictionary
            check_registry: Registry of custom checks
        """
        super().__init__(name, config)
        self._check_registry = check_registry
        self._rules = config.get("rules", [])

    def _execute_rule(
        self, 
        rule: Dict[str, Any], 
        context: ValidationContext
    ) -> CheckResult:
        """
        Execute a single cross-field validation rule.
        
        Args:
            rule: Rule configuration dictionary
            context: Validation context
            
        Returns:
            CheckResult from the check execution
            
        Raises:
            ValueError: If check_id is not found in registry
        """
        check_id = rule["check_id"]
        columns = rule["columns"]
        check_class = self._check_registry.get(check_id)
        
        if check_class is None:
            raise ValueError(f"Custom check '{check_id}' not found in registry")
        
        # Merge default params with rule params
        merged_params = self._check_registry.get_merged_params(
            check_id, 
            rule.get("params", {})
        )
        
        # Instantiate the check
        check_instance = check_class(**merged_params)
        
        # Execute as multi-column validation
        return check_instance.validate_columns(context.data, columns)

    def execute(self, context: ValidationContext) -> StageResult:
        """
        Execute all cross-field validation rules.
        
        Args:
            context: Validation context containing the data
            
        Returns:
            StageResult with validation outcome
        """
        start = time.perf_counter()
        errors: List[ValidationError] = []
        warnings: List[ValidationWarning] = []
        all_failing_indices: set = set()
        
        for rule in self._rules:
            try:
                result = self._execute_rule(rule, context)
                
                if not result.is_valid:
                    check_id = rule["check_id"]
                    columns = rule["columns"]
                    error_message = rule.get(
                        "error_message", 
                        result.message or f"Cross-field validation failed for columns: {columns}"
                    )
                    
                    if result.row_indices:
                        all_failing_indices.update(result.row_indices)
                    
                    errors.append(ValidationError(
                        check_name=check_id,
                        column=", ".join(columns),  # Join columns for display
                        error_message=error_message,
                        failure_cases=result.failure_cases,
                        row_indices=result.row_indices,
                    ))
                    
                    if self.fail_fast:
                        break
            except Exception as e:
                # Handle unexpected errors during check execution
                errors.append(ValidationError(
                    check_name=rule.get("check_id", "unknown"),
                    column=", ".join(rule.get("columns", [])),
                    error_message=f"Cross-field check execution error: {str(e)}",
                    failure_cases=None,
                    row_indices=None,
                ))
                
                if self.fail_fast:
                    break
        
        execution_ms = (time.perf_counter() - start) * 1000
        
        return StageResult(
            stage_name=self.name,
            stage_type=self.stage_type,
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            execution_time_ms=execution_ms,
            rows_validated=len(context.data),
            rows_failed=len(all_failing_indices),
            output_data=None,  # Cross-field validation doesn't modify data
        )

    def validate_config(self, config: Dict[str, Any]) -> Optional[str]:
        """
        Validate the cross-field validation stage configuration.
        
        Args:
            config: Stage configuration dictionary
            
        Returns:
            None if valid, error message if invalid
        """
        if "rules" not in config:
            return "Cross-field validation stage requires 'rules' configuration"
        
        if not config["rules"]:
            return "Cross-field validation stage requires at least one rule"
        
        for i, rule in enumerate(config["rules"]):
            if "check_id" not in rule:
                return f"Rule {i} is missing required 'check_id' field"
            if "columns" not in rule:
                return f"Rule {i} is missing required 'columns' field"
            if not isinstance(rule["columns"], list) or len(rule["columns"]) < 2:
                return f"Rule {i} 'columns' must be a list with at least 2 columns"
        
        return None
