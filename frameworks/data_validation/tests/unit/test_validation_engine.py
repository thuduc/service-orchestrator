"""Unit tests for ValidationEngine."""

from typing import Any, Dict

import polars as pl
import pytest

from frameworks.data_validation.contract.check import CheckResult, CustomCheck
from frameworks.data_validation.engine.validation_engine import ValidationEngine


class PositiveCheck(CustomCheck):
    """Check that values are positive."""

    def validate_column(self, df: pl.DataFrame, column: str) -> CheckResult:
        values = df[column].to_list()
        failures = [v for v in values if v is not None and v <= 0]
        
        if not failures:
            return CheckResult(is_valid=True, message="All values are positive")
        
        mask = df.select((pl.col(column) <= 0) | pl.col(column).is_null()).to_series()
        indices = [i for i, m in enumerate(mask) if m]
        
        return CheckResult(
            is_valid=False,
            message="Some values are not positive",
            failure_cases=failures[:10],
            row_indices=indices,
        )


class TestValidationEngine:
    """Test cases for ValidationEngine."""

    def test_create_engine_without_config(self) -> None:
        """Test creating an engine without configuration files."""
        engine = ValidationEngine()
        
        assert len(engine.list_pipelines()) == 0
        assert "schema_validation" in engine.list_stage_types()
        assert "custom_rules" in engine.list_stage_types()

    def test_add_pipeline_programmatically(self) -> None:
        """Test adding a pipeline programmatically."""
        engine = ValidationEngine()
        
        pipeline_config = {
            "description": "Test pipeline",
            "stages": [
                {
                    "name": "test_stage",
                    "type": "schema_validation",
                    "config": {
                        "columns": {
                            "id": {"dtype": "Int64", "nullable": False},
                        },
                    },
                },
            ],
        }
        
        engine.add_pipeline("test_pipeline", pipeline_config)
        
        assert "test_pipeline" in engine.list_pipelines()
        assert engine.get_pipeline_config("test_pipeline") == pipeline_config

    def test_add_duplicate_pipeline_raises_error(self) -> None:
        """Test that adding a duplicate pipeline raises ValueError."""
        engine = ValidationEngine()
        
        pipeline_config = {"stages": []}
        engine.add_pipeline("test", pipeline_config)
        
        with pytest.raises(ValueError, match="already exists"):
            engine.add_pipeline("test", pipeline_config)

    def test_add_duplicate_pipeline_with_overwrite(self) -> None:
        """Test that overwrite=True allows replacing a pipeline."""
        engine = ValidationEngine()
        
        engine.add_pipeline("test", {"description": "first"})
        engine.add_pipeline("test", {"description": "second"}, overwrite=True)
        
        assert engine.get_pipeline_config("test")["description"] == "second"

    def test_register_custom_check(self) -> None:
        """Test registering a custom check."""
        engine = ValidationEngine()
        engine.register_check("positive_check", PositiveCheck)
        
        assert "positive_check" in engine.list_checks()

    def test_validate_nonexistent_pipeline_raises_error(self) -> None:
        """Test that validating with a non-existent pipeline raises ValueError."""
        engine = ValidationEngine()
        df = pl.DataFrame({"id": [1, 2, 3]})
        
        with pytest.raises(ValueError, match="not found"):
            engine.validate("nonexistent", df)

    def test_validate_simple_schema(self) -> None:
        """Test validation with a simple schema."""
        engine = ValidationEngine()
        
        pipeline_config = {
            "stages": [
                {
                    "name": "validate_schema",
                    "type": "schema_validation",
                    "config": {
                        "columns": {
                            "id": {"dtype": "Int64", "nullable": False},
                            "name": {"dtype": "Utf8", "nullable": False},
                        },
                    },
                },
            ],
        }
        engine.add_pipeline("test", pipeline_config)
        
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        })
        
        result = engine.validate("test", df)
        
        assert result.is_valid
        assert result.total_errors == 0
        assert result.rows_validated == 3

    def test_validate_with_on_failure_fail_fast(self) -> None:
        """Test validation with on_failure=fail_fast stops at first failure."""
        engine = ValidationEngine()
        engine.register_check("positive_check", PositiveCheck)
        
        pipeline_config = {
            "on_failure": "fail_fast",
            "stages": [
                {
                    "name": "stage_1",
                    "type": "custom_rules",
                    "config": {
                        "rules": [
                            {"check_id": "positive_check", "column": "value"},
                        ],
                    },
                },
                {
                    "name": "stage_2",
                    "type": "schema_validation",
                    "config": {
                        "columns": {"id": {"dtype": "Int64"}},
                    },
                },
            ],
        }
        engine.add_pipeline("test", pipeline_config)
        
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "value": [-1, 2, 3],  # First stage will fail
        })
        
        result = engine.validate("test", df)
        
        assert not result.is_valid
        # Only 1 stage executed due to fail_fast
        assert len(result.stage_results) == 1

    def test_validate_with_on_failure_collect_all(self) -> None:
        """Test validation with on_failure=collect_all continues after failure."""
        engine = ValidationEngine()
        engine.register_check("positive_check", PositiveCheck)
        
        pipeline_config = {
            "on_failure": "collect_all",
            "stages": [
                {
                    "name": "stage_1",
                    "type": "custom_rules",
                    "config": {
                        "rules": [
                            {"check_id": "positive_check", "column": "value"},
                        ],
                    },
                },
                {
                    "name": "stage_2",
                    "type": "schema_validation",
                    "config": {
                        "columns": {"id": {"dtype": "Int64"}},
                    },
                },
            ],
        }
        engine.add_pipeline("test", pipeline_config)
        
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "value": [-1, 2, 3],  # First stage will fail
        })
        
        result = engine.validate("test", df)
        
        assert not result.is_valid
        # Both stages executed
        assert len(result.stage_results) == 2

    def test_validation_result_to_dict(self) -> None:
        """Test that ValidationResult.to_dict() returns proper structure."""
        engine = ValidationEngine()
        
        pipeline_config = {
            "stages": [
                {
                    "name": "test_stage",
                    "type": "schema_validation",
                    "config": {
                        "columns": {"id": {"dtype": "Int64"}},
                    },
                },
            ],
        }
        engine.add_pipeline("test", pipeline_config)
        
        df = pl.DataFrame({"id": [1, 2, 3]})
        result = engine.validate("test", df)
        
        result_dict = result.to_dict()
        
        assert "pipeline_id" in result_dict
        assert "is_valid" in result_dict
        assert "stage_results" in result_dict
        assert isinstance(result_dict["stage_results"], list)
