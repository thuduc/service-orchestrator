"""Referential validation stage for cross-dataset validation."""

import time
from typing import Any, Dict, List, Optional

import polars as pl

from frameworks.data_validation.contract.check import CheckResult, CustomCheck
from frameworks.data_validation.engine.validation_context import ValidationContext
from frameworks.data_validation.engine.validation_result import (
    StageResult,
    ValidationError,
    ValidationWarning,
)
from frameworks.data_validation.registries.check_registry import CheckRegistry
from frameworks.data_validation.stages.base_stage import BaseValidationStage


class ReferentialValidationStage(BaseValidationStage):
    """
    Validates relationships between the primary data and reference datasets.
    
    This stage is designed for foreign key-style validations, such as:
    - Customer ID exists in customers table
    - Product code exists in products catalog
    - Category ID exists in categories (with optional condition)
    
    Reference datasets are provided via ValidationContext.reference_data.
    
    Configuration options:
    - fail_fast: If True, stop after the first rule failure
    - rules: List of referential validation rule configurations
    
    Rule configuration:
    - check_id: ID of the custom check in the registry
    - column: Column in the primary data to validate
    - reference_dataset: Name of the reference dataset in context.reference_data
    - reference_column: Column in the reference dataset to match against
    - params: Dictionary of additional parameters for the check
    - error_message: Custom error message (overrides check default)
    """

    stage_type = "referential_validation"

    def __init__(
        self, 
        name: str, 
        config: Dict[str, Any],
        check_registry: CheckRegistry,
    ) -> None:
        """
        Initialize the referential validation stage.
        
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
        Execute a single referential validation rule.
        
        Args:
            rule: Rule configuration dictionary
            context: Validation context with reference data
            
        Returns:
            CheckResult from the check execution
            
        Raises:
            ValueError: If check_id not found or reference_dataset missing
        """
        check_id = rule["check_id"]
        column = rule["column"]
        reference_dataset = rule["reference_dataset"]
        reference_column = rule["reference_column"]
        
        # Get reference data
        if reference_dataset not in context.reference_data:
            return CheckResult(
                is_valid=False,
                message=f"Reference dataset '{reference_dataset}' not found in context",
                failure_cases=None,
                row_indices=None,
            )
        
        ref_df = context.reference_data[reference_dataset]
        
        if reference_column not in ref_df.columns:
            return CheckResult(
                is_valid=False,
                message=f"Reference column '{reference_column}' not found in dataset '{reference_dataset}'",
                failure_cases=None,
                row_indices=None,
            )
        
        check_class = self._check_registry.get(check_id)
        
        if check_class is None:
            raise ValueError(f"Custom check '{check_id}' not found in registry")
        
        # Merge default params with rule params
        base_params = rule.get("params", {})
        base_params["reference_data"] = ref_df
        base_params["reference_column"] = reference_column
        
        merged_params = self._check_registry.get_merged_params(check_id, base_params)
        
        # Instantiate the check
        check_instance = check_class(**merged_params)
        
        # Execute as column validation
        return check_instance.validate_column(context.data, column)

    def execute(self, context: ValidationContext) -> StageResult:
        """
        Execute all referential validation rules.
        
        Args:
            context: Validation context containing primary data and reference datasets
            
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
                    column = rule["column"]
                    reference_dataset = rule["reference_dataset"]
                    reference_column = rule["reference_column"]
                    
                    error_message = rule.get(
                        "error_message", 
                        result.message or (
                            f"Referential integrity check failed: "
                            f"'{column}' values not found in "
                            f"'{reference_dataset}.{reference_column}'"
                        )
                    )
                    
                    if result.row_indices:
                        all_failing_indices.update(result.row_indices)
                    
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
                    error_message=f"Referential check execution error: {str(e)}",
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
            output_data=None,  # Referential validation doesn't modify data
        )

    def validate_config(self, config: Dict[str, Any]) -> Optional[str]:
        """
        Validate the referential validation stage configuration.
        
        Args:
            config: Stage configuration dictionary
            
        Returns:
            None if valid, error message if invalid
        """
        if "rules" not in config:
            return "Referential validation stage requires 'rules' configuration"
        
        if not config["rules"]:
            return "Referential validation stage requires at least one rule"
        
        for i, rule in enumerate(config["rules"]):
            if "check_id" not in rule:
                return f"Rule {i} is missing required 'check_id' field"
            if "column" not in rule:
                return f"Rule {i} is missing required 'column' field"
            if "reference_dataset" not in rule:
                return f"Rule {i} is missing required 'reference_dataset' field"
            if "reference_column" not in rule:
                return f"Rule {i} is missing required 'reference_column' field"
        
        return None
