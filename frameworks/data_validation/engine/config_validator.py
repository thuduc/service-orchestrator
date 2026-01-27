"""ConfigValidator - Pre-execution validation of pipeline and check configurations."""

import difflib
import inspect
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from frameworks.data_validation.registries.check_registry import CheckRegistry
    from frameworks.data_validation.registries.stage_registry import StageRegistry


# Valid Polars data types
VALID_POLARS_DTYPES = {
    "Int8", "Int16", "Int32", "Int64",
    "UInt8", "UInt16", "UInt32", "UInt64",
    "Float32", "Float64",
    "Boolean", "Utf8", "String",
    "Date", "Datetime", "Time", "Duration",
    "Categorical", "Null", "Object",
}

# Valid Pandera built-in checks
VALID_PANDERA_CHECKS = {
    "greater_than", "greater_than_or_equal_to",
    "less_than", "less_than_or_equal_to",
    "in_range", "equal_to", "not_equal_to",
    "isin", "notin",
    "str_matches", "str_contains", "str_startswith", "str_endswith", "str_length",
}

# Stage-specific validation rules
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


@dataclass
class ConfigValidationError:
    """Represents a configuration validation error."""

    path: str  # JSON path to the error location
    message: str  # Human-readable error message
    severity: str  # "error" or "warning"


class ConfigurationError(Exception):
    """Raised when configuration validation fails."""
    pass


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

    def __init__(
        self,
        check_registry: "CheckRegistry",
        stage_registry: "StageRegistry",
    ):
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
        config: Dict[str, Any],
        pipeline_path: str = "",
    ) -> List[ConfigValidationError]:
        """
        Validate a pipeline configuration.

        Args:
            config: Pipeline configuration dictionary
            pipeline_path: Path prefix for error messages

        Returns:
            List of validation errors (empty if valid)
        """
        errors = []

        # Validate pipeline has stages
        if "stages" not in config or not config["stages"]:
            errors.append(ConfigValidationError(
                path=f"{pipeline_path}.stages" if pipeline_path else "stages",
                message="Pipeline must have at least one stage",
                severity="error"
            ))
            return errors

        # Validate each stage
        for i, stage in enumerate(config["stages"]):
            stage_path = f"{pipeline_path}.stages[{i}]" if pipeline_path else f"stages[{i}]"

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
        path: str,
    ) -> List[ConfigValidationError]:
        """Validate stage-specific configuration."""
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

        # Validate schema_validation specific config
        if stage_type == "schema_validation" and "columns" in config:
            errors.extend(self._validate_columns_config(config["columns"], f"{path}.columns"))

        # Validate check_id references for rule-based stages
        if "rules" in config:
            for i, rule in enumerate(config["rules"]):
                if "check_id" in rule:
                    check_id = rule["check_id"]
                    if not self._check_registry.has(check_id):
                        errors.append(ConfigValidationError(
                            path=f"{path}.rules[{i}].check_id",
                            message=f"Referenced check '{check_id}' not found in check registry",
                            severity="error"
                        ))

        return errors

    def _validate_columns_config(
        self,
        columns: Dict[str, Any],
        path: str,
    ) -> List[ConfigValidationError]:
        """Validate column configurations for schema_validation stage."""
        errors = []

        for col_name, col_config in columns.items():
            col_path = f"{path}.{col_name}"

            # Validate dtype
            if "dtype" in col_config:
                dtype_error = self._validate_dtype(col_config["dtype"], f"{col_path}.dtype")
                if dtype_error:
                    errors.append(dtype_error)

            # Validate checks
            if "checks" in col_config:
                for i, check in enumerate(col_config["checks"]):
                    if "builtin" in check:
                        check_error = self._validate_builtin_check(
                            check["builtin"],
                            f"{col_path}.checks[{i}].builtin"
                        )
                        if check_error:
                            errors.append(check_error)

        return errors

    def _validate_dtype(
        self,
        dtype: str,
        path: str,
    ) -> Optional[ConfigValidationError]:
        """Validate a Polars dtype string."""
        if dtype not in VALID_POLARS_DTYPES:
            suggestions = difflib.get_close_matches(dtype, VALID_POLARS_DTYPES, n=1)
            suggestion_text = f" - did you mean '{suggestions[0]}'?" if suggestions else ""
            return ConfigValidationError(
                path=path,
                message=f"Unknown dtype '{dtype}'{suggestion_text}",
                severity="error"
            )
        return None

    def _validate_builtin_check(
        self,
        check_name: str,
        path: str,
    ) -> Optional[ConfigValidationError]:
        """Validate a Pandera built-in check name."""
        if check_name not in VALID_PANDERA_CHECKS:
            return ConfigValidationError(
                path=path,
                message=f"Unknown Pandera check '{check_name}'",
                severity="error"
            )
        return None

    def validate_check_config(
        self,
        check_id: str,
        config: Dict[str, Any],
        path: str = "",
    ) -> List[ConfigValidationError]:
        """
        Validate a custom check configuration.

        Args:
            check_id: ID of the check
            config: Check configuration dictionary
            path: Path prefix for error messages

        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        check_path = f"{path}.{check_id}" if path else check_id

        # Required fields
        for field in ["module", "class"]:
            if field not in config:
                errors.append(ConfigValidationError(
                    path=f"{check_path}.{field}",
                    message=f"Check configuration requires field '{field}'",
                    severity="error"
                ))

        return errors

    def validate_check_params(
        self,
        check_id: str,
        params: Dict[str, Any],
        path: str,
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

    def validate_all(
        self,
        pipelines_config: Dict[str, Any],
        checks_config: Optional[Dict[str, Any]] = None,
    ) -> List[ConfigValidationError]:
        """
        Validate all configurations.

        Args:
            pipelines_config: Full pipelines configuration file content
            checks_config: Full checks configuration file content (optional)

        Returns:
            List of all validation errors across all configs
        """
        errors = []

        # Validate pipelines
        pipelines = pipelines_config.get("validation_pipelines", {})
        for pipeline_id, pipeline_config in pipelines.items():
            errors.extend(self.validate_pipeline_config(
                pipeline_config,
                f"validation_pipelines.{pipeline_id}"
            ))

        # Validate checks config
        if checks_config:
            checks = checks_config.get("custom_checks", {})
            for check_id, check_config in checks.items():
                errors.extend(self.validate_check_config(
                    check_id,
                    check_config,
                    "custom_checks"
                ))

        return errors
