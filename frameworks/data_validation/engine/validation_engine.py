"""ValidationEngine - Main orchestration engine for validation pipelines."""

import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, Union

import polars as pl

from frameworks.data_validation.contract.check import CustomCheck
from frameworks.data_validation.contract.validation_stage import ValidationStage
from frameworks.data_validation.engine.config_validator import ConfigValidator
from frameworks.data_validation.engine.validation_context import ValidationContext
from frameworks.data_validation.engine.validation_result import ValidationResult
from frameworks.data_validation.loaders.config_loader import ConfigLoader
from frameworks.data_validation.registries.check_registry import CheckRegistry
from frameworks.data_validation.registries.stage_registry import StageRegistry
from frameworks.data_validation.stages.schema_validation import SchemaValidationStage
from frameworks.data_validation.stages.custom_rules import CustomRulesStage
from frameworks.data_validation.stages.cross_field_validation import CrossFieldValidationStage
from frameworks.data_validation.stages.referential_validation import ReferentialValidationStage


logger = logging.getLogger(__name__)


class ConfigurationError(Exception):
    """Raised when configuration validation fails."""
    pass


class ValidationEngine:
    """
    Main orchestration engine for validation pipelines.
    
    The ValidationEngine is responsible for:
    - Loading validation pipeline configurations
    - Managing the check registry and stage registry
    - Executing validation stages in sequence
    - Aggregating results from all stages
    
    Example usage:
        ```python
        engine = ValidationEngine(
            pipeline_config_path="config/validation_pipelines.json",
            check_config_path="config/custom_checks.json",
        )
        
        result = engine.validate(
            pipeline_id="customer_validation",
            data=customers_df,
            context={"reference_data": {"products": products_df}},
        )
        
        if not result.is_valid:
            print(f"Validation failed with {result.total_errors} errors")
        ```
    """

    # Built-in stage type to class mapping
    BUILTIN_STAGES = {
        "schema_validation": SchemaValidationStage,
        "custom_rules": CustomRulesStage,
        "cross_field_validation": CrossFieldValidationStage,
        "referential_validation": ReferentialValidationStage,
    }

    def __init__(
        self,
        pipeline_config_path: Optional[Union[str, Path]] = None,
        check_config_path: Optional[Union[str, Path]] = None,
        validate_config: bool = True,
    ) -> None:
        """
        Initialize the validation engine.
        
        Args:
            pipeline_config_path: Path to validation pipelines JSON/YAML config
            check_config_path: Path to custom checks JSON/YAML config
            validate_config: If True, validate configurations at load time
            
        Raises:
            ConfigurationError: If configuration validation fails
        """
        self._pipelines: Dict[str, Any] = {}
        self._pipelines_config: Dict[str, Any] = {}
        self._checks_config: Dict[str, Any] = {}
        self._check_registry = CheckRegistry()
        self._stage_registry = StageRegistry()
        
        # Register built-in stages
        self._register_builtin_stages()
        
        # Load configurations if provided
        if pipeline_config_path:
            self._load_pipeline_config(pipeline_config_path)
        
        if check_config_path:
            self._load_check_config(check_config_path)
        
        # Validate configurations
        if validate_config and (self._pipelines_config or self._checks_config):
            self._validate_configurations()

    def _register_builtin_stages(self) -> None:
        """Register built-in stage types."""
        for stage_type, stage_class in self.BUILTIN_STAGES.items():
            self._stage_registry.register(stage_type, stage_class)

    def _load_pipeline_config(self, path: Union[str, Path]) -> None:
        """
        Load pipeline configurations from file.
        
        Args:
            path: Path to the pipelines configuration file
        """
        self._pipelines_config = ConfigLoader.load(path)
        self._pipelines = self._pipelines_config.get("validation_pipelines", {})
        logger.info(f"Loaded {len(self._pipelines)} validation pipelines from {path}")

    def _load_check_config(self, path: Union[str, Path]) -> None:
        """
        Load custom check configurations from file.
        
        This method loads check definitions and registers them. Check definitions
        can either reference a module/class or provide inline configuration.
        
        Args:
            path: Path to the checks configuration file
        """
        self._checks_config = ConfigLoader.load(path)
        checks = self._checks_config.get("custom_checks", {})
        
        for check_id, check_def in checks.items():
            if "module" in check_def and "class" in check_def:
                # Dynamic import of check class
                check_class = self._import_check_class(
                    check_def["module"], 
                    check_def["class"]
                )
                default_params = check_def.get("default_params", {})
                self._check_registry.register(check_id, check_class, default_params)
            else:
                logger.warning(
                    f"Check '{check_id}' missing 'module' and 'class' - skipping"
                )
        
        logger.info(f"Loaded {len(checks)} custom check definitions from {path}")

    def _import_check_class(self, module_path: str, class_name: str) -> Type[CustomCheck]:
        """
        Dynamically import a check class.
        
        Args:
            module_path: Full module path (e.g., 'myproject.checks.custom')
            class_name: Class name within the module
            
        Returns:
            The check class
            
        Raises:
            ImportError: If module or class cannot be imported
        """
        import importlib
        
        try:
            module = importlib.import_module(module_path)
            check_class = getattr(module, class_name)
            
            if not (isinstance(check_class, type) and issubclass(check_class, CustomCheck)):
                raise TypeError(
                    f"'{class_name}' from '{module_path}' is not a CustomCheck subclass"
                )
            
            return check_class
        except ImportError as e:
            raise ImportError(f"Cannot import module '{module_path}': {e}")
        except AttributeError:
            raise ImportError(f"Class '{class_name}' not found in module '{module_path}'")

    def _validate_configurations(self) -> None:
        """
        Validate all loaded configurations.
        
        Raises:
            ConfigurationError: If critical errors are found
        """
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
            raise ConfigurationError(f"Invalid configuration:\n{error_messages}")

    def _create_stage(
        self, 
        stage_config: Dict[str, Any]
    ) -> ValidationStage:
        """
        Create a stage instance from configuration.
        
        Args:
            stage_config: Stage configuration dictionary
            
        Returns:
            Configured stage instance
            
        Raises:
            ValueError: If stage type is unknown
        """
        stage_type = stage_config["type"]
        stage_name = stage_config["name"]
        config = stage_config.get("config", {})
        
        stage_class = self._stage_registry.get(stage_type)
        if stage_class is None:
            raise ValueError(f"Unknown stage type: {stage_type}")
        
        # Some stages need the check registry
        if stage_type in ("custom_rules", "cross_field_validation", "referential_validation"):
            return stage_class(stage_name, config, self._check_registry)
        else:
            return stage_class(stage_name, config)

    def validate(
        self,
        pipeline_id: str,
        data: pl.DataFrame,
        context: Optional[Dict[str, Any]] = None,
    ) -> ValidationResult:
        """
        Execute a validation pipeline on the given data.
        
        This method:
        1. Initializes a ValidationContext with the input data
        2. Executes each stage in sequence
        3. Propagates data changes between stages (for coercing stages)
        4. Respects on_failure behavior (fail_fast vs collect_all)
        5. Returns aggregated ValidationResult
        
        Args:
            pipeline_id: ID of the validation pipeline to execute
            data: Polars DataFrame to validate
            context: Optional context data containing:
                - reference_data: Dict of reference DataFrames for lookups
                - metadata: Additional metadata to pass to stages
                
        Returns:
            ValidationResult containing all stage results
            
        Raises:
            ValueError: If pipeline_id is not found
        """
        if pipeline_id not in self._pipelines:
            available = ", ".join(self._pipelines.keys()) if self._pipelines else "none"
            raise ValueError(
                f"Pipeline '{pipeline_id}' not found. Available pipelines: {available}"
            )
        
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
        
        for stage_config in pipeline_config.get("stages", []):
            ctx.current_stage = stage_config["name"]
            
            try:
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
                    logger.info(
                        f"Pipeline '{pipeline_id}' stopping at stage '{stage_config['name']}' "
                        f"due to on_failure=fail_fast"
                    )
                    break
            except Exception as e:
                # Handle unexpected stage execution errors
                from frameworks.data_validation.engine.validation_result import (
                    StageResult, 
                    ValidationError
                )
                
                error_result = StageResult(
                    stage_name=stage_config["name"],
                    stage_type=stage_config["type"],
                    is_valid=False,
                    errors=[ValidationError(
                        check_name="stage_execution",
                        column=None,
                        error_message=f"Stage execution failed: {str(e)}",
                        failure_cases=None,
                        row_indices=None,
                    )],
                    warnings=[],
                    execution_time_ms=(time.perf_counter() - stage_start) * 1000,
                    rows_validated=len(ctx.data),
                    rows_failed=0,
                    output_data=None,
                )
                ctx.add_stage_result(error_result)
                
                if on_failure == "fail_fast":
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

    def register_check(
        self, 
        check_id: str, 
        check_class: Type[CustomCheck],
        default_params: Optional[Dict[str, Any]] = None,
        overwrite: bool = False,
    ) -> None:
        """
        Register a custom check class.
        
        Args:
            check_id: Unique identifier for the check
            check_class: The check class (must be subclass of CustomCheck)
            default_params: Optional default parameters for the check
            overwrite: If True, replace existing registration
        """
        self._check_registry.register(check_id, check_class, default_params, overwrite)

    def register_stage(
        self,
        stage_type: str,
        stage_class: Type[ValidationStage],
        overwrite: bool = False,
    ) -> None:
        """
        Register a custom stage type.
        
        Args:
            stage_type: Unique identifier for the stage type
            stage_class: The stage class
            overwrite: If True, replace existing registration
        """
        self._stage_registry.register(stage_type, stage_class, overwrite)

    def add_pipeline(
        self,
        pipeline_id: str,
        pipeline_config: Dict[str, Any],
        overwrite: bool = False,
    ) -> None:
        """
        Add a pipeline configuration programmatically.
        
        Args:
            pipeline_id: Unique identifier for the pipeline
            pipeline_config: Pipeline configuration dictionary
            overwrite: If True, replace existing pipeline
            
        Raises:
            ValueError: If pipeline_id already exists and overwrite is False
        """
        if pipeline_id in self._pipelines and not overwrite:
            raise ValueError(
                f"Pipeline '{pipeline_id}' already exists. Use overwrite=True to replace."
            )
        
        self._pipelines[pipeline_id] = pipeline_config
        logger.info(f"Added pipeline '{pipeline_id}'")

    def list_pipelines(self) -> List[str]:
        """
        List all registered validation pipelines.
        
        Returns:
            List of pipeline IDs
        """
        return list(self._pipelines.keys())

    def list_checks(self) -> List[str]:
        """
        List all registered custom checks.
        
        Returns:
            List of check IDs
        """
        return self._check_registry.list_checks()

    def list_stage_types(self) -> List[str]:
        """
        List all registered stage types.
        
        Returns:
            List of stage type identifiers
        """
        return self._stage_registry.list_stages()

    def get_pipeline_config(self, pipeline_id: str) -> Optional[Dict[str, Any]]:
        """
        Get configuration for a specific pipeline.
        
        Args:
            pipeline_id: ID of the pipeline
            
        Returns:
            Pipeline configuration dictionary, or None if not found
        """
        return self._pipelines.get(pipeline_id)
