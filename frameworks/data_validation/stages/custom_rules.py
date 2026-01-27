"""Custom rules validation stage."""

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


class CustomRulesStage(BaseValidationStage):
    """
    Executes custom validation rules.
    
    This stage runs custom checks that are registered in the CheckRegistry.
    Each rule references a check by ID and can provide parameters to customize
    the check behavior.
    
    Configuration options:
    - fail_fast: If True, stop after the first rule failure
    - rules: List of rule configurations
    
    Rule configuration:
    - check_id: ID of the custom check in the registry
    - column: Single column name (for column-level checks)
    - columns: List of column names (for multi-column checks)
    - params: Dictionary of parameters to pass to the check
    - error_message: Custom error message (overrides check default)
    - raise_warning: If True, raise as warning instead of error
    """

    stage_type = "custom_rules"

    def __init__(
        self, 
        name: str, 
        config: Dict[str, Any],
        check_registry: CheckRegistry,
    ) -> None:
        """
        Initialize the custom rules stage.
        
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
        Execute a single validation rule.
        
        Args:
            rule: Rule configuration dictionary
            context: Validation context
            
        Returns:
            CheckResult from the check execution
            
        Raises:
            ValueError: If check_id is not found in registry
        """
        check_id = rule["check_id"]
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
        
        # Determine check type and execute
        if "column" in rule:
            return check_instance.validate_column(context.data, rule["column"])
        elif "columns" in rule:
            return check_instance.validate_columns(context.data, rule["columns"])
        else:
            return check_instance.validate_dataframe(context.data)

    def execute(self, context: ValidationContext) -> StageResult:
        """
        Execute all custom validation rules.
        
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
                    error_message = rule.get("error_message", result.message)
                    column = rule.get("column")
                    
                    if result.row_indices:
                        all_failing_indices.update(result.row_indices)
                    
                    if rule.get("raise_warning", False):
                        warnings.append(ValidationWarning(
                            check_name=check_id,
                            column=column,
                            message=error_message,
                            row_count=len(result.row_indices) if result.row_indices else 0,
                        ))
                    else:
                        errors.append(ValidationError(
                            check_name=check_id,
                            column=column,
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
                    column=rule.get("column"),
                    error_message=f"Check execution error: {str(e)}",
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
            output_data=None,  # Custom rules don't modify data
        )

    def validate_config(self, config: Dict[str, Any]) -> Optional[str]:
        """
        Validate the custom rules stage configuration.
        
        Args:
            config: Stage configuration dictionary
            
        Returns:
            None if valid, error message if invalid
        """
        if "rules" not in config:
            return "Custom rules stage requires 'rules' configuration"
        
        if not config["rules"]:
            return "Custom rules stage requires at least one rule"
        
        for i, rule in enumerate(config["rules"]):
            if "check_id" not in rule:
                return f"Rule {i} is missing required 'check_id' field"
        
        return None
