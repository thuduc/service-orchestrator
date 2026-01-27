"""Tests for TransformationEngine."""

import pytest
import polars as pl
from pathlib import Path
import tempfile
import json

from frameworks.data_transformation.engine.transformation_engine import TransformationEngine
from frameworks.data_transformation.engine.transformation_context import TransformationContext
from frameworks.data_transformation.contract.transformer import Transformer
from frameworks.data_transformation.exceptions import (
    PipelineNotFoundError,
    ConfigurationError,
    DatasetNotFoundError,
)


class TestTransformationEngineInit:
    """Tests for TransformationEngine initialization."""
    
    def test_engine_initializes_with_defaults(self):
        """Test engine initializes without config path."""
        engine = TransformationEngine()
        
        assert engine.list_pipelines() == []
        assert len(engine.list_transformers()) > 0  # Built-in transformers
    
    def test_engine_registers_builtin_transformers(self):
        """Test that all built-in transformers are registered."""
        engine = TransformationEngine()
        
        expected_transformers = [
            "select", "drop", "rename", "cast", "with_columns",
            "filter", "sort", "unique", "head", "tail", "slice", "sample", "drop_nulls",
            "pivot", "unpivot", "explode",
            "group_by",
            "join", "concat", "union",
            "fill_null", "fill_nan",
        ]
        
        registered = engine.list_transformers()
        for transformer in expected_transformers:
            assert transformer in registered, f"Missing transformer: {transformer}"
    
    def test_engine_loads_config_from_file(self, tmp_path):
        """Test engine loads pipeline config from JSON file."""
        config = {
            "pipelines": {
                "test_pipeline": {
                    "steps": [
                        {"name": "select_cols", "type": "select", "config": {"columns": ["name"]}}
                    ]
                }
            }
        }
        config_path = tmp_path / "pipelines.json"
        config_path.write_text(json.dumps(config))
        
        engine = TransformationEngine(pipeline_config_path=config_path)
        
        assert "test_pipeline" in engine.list_pipelines()


class TestTransformationEnginePipelineManagement:
    """Tests for pipeline management."""
    
    def test_add_pipeline(self, transformation_engine):
        """Test adding a pipeline programmatically."""
        pipeline_config = {
            "steps": [
                {"name": "select", "type": "select", "config": {"columns": ["name"]}}
            ]
        }
        
        transformation_engine.add_pipeline("my_pipeline", pipeline_config)
        
        assert "my_pipeline" in transformation_engine.list_pipelines()
    
    def test_add_pipeline_no_overwrite(self, transformation_engine):
        """Test adding duplicate pipeline without overwrite raises error."""
        pipeline_config = {"steps": []}
        
        transformation_engine.add_pipeline("dup_pipeline", pipeline_config)
        
        with pytest.raises(ValueError, match="already exists"):
            transformation_engine.add_pipeline("dup_pipeline", pipeline_config)
    
    def test_add_pipeline_with_overwrite(self, transformation_engine):
        """Test adding duplicate pipeline with overwrite succeeds."""
        pipeline_config_v1 = {"steps": [], "version": 1}
        pipeline_config_v2 = {"steps": [], "version": 2}
        
        transformation_engine.add_pipeline("my_pipeline", pipeline_config_v1)
        transformation_engine.add_pipeline("my_pipeline", pipeline_config_v2, overwrite=True)
        
        config = transformation_engine.get_pipeline_config("my_pipeline")
        assert config["version"] == 2
    
    def test_get_pipeline_config(self, transformation_engine):
        """Test retrieving pipeline config."""
        pipeline_config = {"steps": [], "description": "Test pipeline"}
        
        transformation_engine.add_pipeline("test_pipeline", pipeline_config)
        
        retrieved = transformation_engine.get_pipeline_config("test_pipeline")
        assert retrieved == pipeline_config
    
    def test_get_pipeline_config_not_found(self, transformation_engine):
        """Test retrieving non-existent pipeline returns None."""
        result = transformation_engine.get_pipeline_config("nonexistent")
        
        assert result is None


class TestTransformationEngineExecution:
    """Tests for pipeline execution."""
    
    def test_transform_simple_pipeline(
        self, 
        transformation_engine, 
        sample_customers_df,
    ):
        """Test executing a simple single-step pipeline."""
        transformation_engine.add_pipeline("select_pipeline", {
            "steps": [
                {"name": "select_cols", "type": "select", "config": {"columns": ["customer_id", "name"]}}
            ]
        })
        
        result = transformation_engine.transform(
            pipeline_id="select_pipeline",
            data=sample_customers_df,
        )
        
        assert result.success is True
        assert result.data is not None
        assert result.data.columns == ["customer_id", "name"]
        assert result.rows_in == 5
        assert result.rows_out == 5
    
    def test_transform_multi_step_pipeline(
        self, 
        transformation_engine, 
        sample_customers_df,
    ):
        """Test executing a multi-step pipeline."""
        transformation_engine.add_pipeline("multi_step", {
            "steps": [
                {"name": "filter_active", "type": "filter", "config": {"condition": "col('status') == 'active'"}},
                {"name": "select_cols", "type": "select", "config": {"columns": ["customer_id", "name", "age"]}},
                {"name": "sort_age", "type": "sort", "config": {"by": "age", "descending": True}},
            ]
        })
        
        result = transformation_engine.transform(
            pipeline_id="multi_step",
            data=sample_customers_df,
        )
        
        assert result.success is True
        assert result.data is not None
        assert len(result.data) == 3  # 3 active customers
        assert result.data.columns == ["customer_id", "name", "age"]
        # Check sorted descending by age
        ages = result.data["age"].to_list()
        assert ages == sorted(ages, reverse=True)
    
    def test_transform_pipeline_not_found(
        self, 
        transformation_engine, 
        sample_customers_df,
    ):
        """Test executing non-existent pipeline raises error."""
        with pytest.raises(PipelineNotFoundError):
            transformation_engine.transform(
                pipeline_id="nonexistent",
                data=sample_customers_df,
            )
    
    def test_transform_with_datasets(
        self, 
        transformation_engine, 
        sample_customers_df,
        sample_orders_df,
    ):
        """Test pipeline execution with additional datasets."""
        transformation_engine.add_pipeline("join_pipeline", {
            "steps": [
                {
                    "name": "join_orders",
                    "type": "join",
                    "config": {
                        "right_dataset": "orders",
                        "on": "customer_id",
                        "how": "left",
                    }
                }
            ]
        })
        
        result = transformation_engine.transform(
            pipeline_id="join_pipeline",
            data=sample_customers_df,
            datasets={"orders": sample_orders_df},
        )
        
        assert result.success is True
        assert result.data is not None
        assert "order_id" in result.data.columns
    
    def test_transform_missing_required_dataset(
        self, 
        transformation_engine, 
        sample_customers_df,
    ):
        """Test pipeline fails when required dataset is missing."""
        transformation_engine.add_pipeline("join_pipeline", {
            "steps": [
                {
                    "name": "join_orders",
                    "type": "join",
                    "config": {
                        "right_dataset": "orders",
                        "on": "customer_id",
                        "how": "left",
                    }
                }
            ]
        })
        
        result = transformation_engine.transform(
            pipeline_id="join_pipeline",
            data=sample_customers_df,
            datasets={},  # Missing 'orders' dataset
        )
        
        assert result.success is False
        assert result.error_message is not None
    
    def test_transform_records_step_results(
        self, 
        transformation_engine, 
        sample_customers_df,
    ):
        """Test that step results are properly recorded."""
        transformation_engine.add_pipeline("tracked_pipeline", {
            "steps": [
                {"name": "filter_step", "type": "filter", "config": {"condition": "col('age') > 25"}},
                {"name": "select_step", "type": "select", "config": {"columns": ["name", "age"]}},
            ]
        })
        
        result = transformation_engine.transform(
            pipeline_id="tracked_pipeline",
            data=sample_customers_df,
        )
        
        assert len(result.step_results) == 2
        
        filter_result = result.step_results[0]
        assert filter_result.step_name == "filter_step"
        assert filter_result.transformer_type == "filter"
        assert filter_result.success is True
        assert filter_result.execution_time_ms >= 0
        
        select_result = result.step_results[1]
        assert select_result.step_name == "select_step"
        assert select_result.columns_out == 2


class TestTransformationEngineErrorHandling:
    """Tests for error handling in pipeline execution."""
    
    def test_transform_step_failure_stops_pipeline(
        self, 
        transformation_engine, 
        sample_customers_df,
    ):
        """Test that step failure stops pipeline and returns error result."""
        transformation_engine.add_pipeline("failing_pipeline", {
            "steps": [
                {"name": "select_valid", "type": "select", "config": {"columns": ["name"]}},
                {"name": "select_invalid", "type": "select", "config": {"columns": ["nonexistent"]}},
                {"name": "never_reached", "type": "select", "config": {"columns": ["name"]}},
            ]
        })
        
        result = transformation_engine.transform(
            pipeline_id="failing_pipeline",
            data=sample_customers_df,
        )
        
        assert result.success is False
        assert result.error_message is not None
        assert len(result.step_results) == 2  # First two steps only
    
    def test_transform_invalid_transformer_type(
        self, 
        transformation_engine, 
        sample_customers_df,
    ):
        """Test that unknown transformer type fails gracefully."""
        transformation_engine.add_pipeline("invalid_type_pipeline", {
            "steps": [
                {"name": "unknown", "type": "nonexistent_transformer", "config": {}}
            ]
        })
        
        result = transformation_engine.transform(
            pipeline_id="invalid_type_pipeline",
            data=sample_customers_df,
        )
        
        assert result.success is False
        assert "nonexistent_transformer" in result.error_message
    
    def test_transform_config_validation_failure(
        self, 
        transformation_engine, 
        sample_customers_df,
    ):
        """Test that config validation failures are handled."""
        transformation_engine.add_pipeline("invalid_config_pipeline", {
            "steps": [
                {"name": "select_no_columns", "type": "select", "config": {}}  # Missing required 'columns'
            ]
        })
        
        result = transformation_engine.transform(
            pipeline_id="invalid_config_pipeline",
            data=sample_customers_df,
        )
        
        assert result.success is False
        assert "columns" in result.error_message.lower()


class TestTransformationEngineCustomTransformers:
    """Tests for custom transformer registration."""
    
    def test_register_custom_transformer(self, transformation_engine):
        """Test registering a custom transformer."""
        class CustomTransformer(Transformer):
            @property
            def transformer_type(self) -> str:
                return "custom"
            
            def transform(self, data, context):
                return data.with_columns(pl.lit("custom").alias("custom_col"))
            
            def validate_config(self, config):
                return None
        
        transformation_engine.register_transformer("custom", CustomTransformer)
        
        assert "custom" in transformation_engine.list_transformers()
    
    def test_use_custom_transformer_in_pipeline(
        self, 
        transformation_engine,
        sample_customers_df,
    ):
        """Test using a custom transformer in a pipeline."""
        class AddConstantTransformer(Transformer):
            def __init__(self, name: str, config: dict):
                self.name = name
                self.config = config
            
            @property
            def transformer_type(self) -> str:
                return "add_constant"
            
            def transform(self, data, context):
                value = self.config.get("value", "default")
                column_name = self.config.get("column_name", "constant")
                return data.with_columns(pl.lit(value).alias(column_name))
            
            def validate_config(self, config):
                return None
        
        transformation_engine.register_transformer("add_constant", AddConstantTransformer)
        transformation_engine.add_pipeline("custom_pipeline", {
            "steps": [
                {"name": "add_const", "type": "add_constant", "config": {"value": "hello", "column_name": "greeting"}}
            ]
        })
        
        result = transformation_engine.transform(
            pipeline_id="custom_pipeline",
            data=sample_customers_df,
        )
        
        assert result.success is True
        assert "greeting" in result.data.columns
        assert result.data["greeting"][0] == "hello"


class TestTransformationResultMetrics:
    """Tests for transformation result metrics."""
    
    def test_result_includes_timing(
        self, 
        transformation_engine, 
        sample_customers_df,
    ):
        """Test that results include execution timing."""
        transformation_engine.add_pipeline("timed_pipeline", {
            "steps": [
                {"name": "select", "type": "select", "config": {"columns": ["name"]}}
            ]
        })
        
        result = transformation_engine.transform(
            pipeline_id="timed_pipeline",
            data=sample_customers_df,
        )
        
        assert result.total_execution_time_ms > 0
        assert result.step_results[0].execution_time_ms >= 0
    
    def test_result_includes_row_counts(
        self, 
        transformation_engine, 
        sample_customers_df,
    ):
        """Test that results include row counts."""
        transformation_engine.add_pipeline("filter_pipeline", {
            "steps": [
                {"name": "filter", "type": "filter", "config": {"condition": "col('status') == 'active'"}}
            ]
        })
        
        result = transformation_engine.transform(
            pipeline_id="filter_pipeline",
            data=sample_customers_df,
        )
        
        assert result.rows_in == 5
        assert result.rows_out == 3  # 3 active customers
        
        step_result = result.step_results[0]
        assert step_result.rows_in == 5
        assert step_result.rows_out == 3
    
    def test_result_includes_column_counts(
        self, 
        transformation_engine, 
        sample_customers_df,
    ):
        """Test that results include column counts."""
        transformation_engine.add_pipeline("select_pipeline", {
            "steps": [
                {"name": "select", "type": "select", "config": {"columns": ["name", "email"]}}
            ]
        })
        
        result = transformation_engine.transform(
            pipeline_id="select_pipeline",
            data=sample_customers_df,
        )
        
        step_result = result.step_results[0]
        assert step_result.columns_in == 6  # Original columns
        assert step_result.columns_out == 2  # Selected columns
