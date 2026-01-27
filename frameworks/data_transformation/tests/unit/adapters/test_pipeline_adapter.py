"""Tests for DataTransformationComponent Service Pipeline adapter."""

import pytest
import polars as pl

from frameworks.data_transformation.adapters.pipeline_adapter import (
    DataTransformationComponent,
    DataTransformationError,
)
from frameworks.data_transformation.engine.pipeline_builder import Pipeline
from frameworks.service_pipeline.implementation.base_component import BaseComponent


class TestDataTransformationComponentInit:
    """Tests for component initialization."""
    
    def test_inherits_from_base_component(self):
        """Test that component properly inherits from BaseComponent."""
        component = DataTransformationComponent({"pipeline_id": "test"})
        
        assert isinstance(component, BaseComponent)
    
    def test_initializes_with_config(self):
        """Test component initializes with configuration."""
        config = {
            "pipeline_id": "test_pipeline",
            "input_key": "input_data",
            "output_key": "output_data",
            "fail_on_error": False,
        }
        
        component = DataTransformationComponent(config)
        
        assert component._pipeline_id == "test_pipeline"
        assert component._input_key == "input_data"
        assert component._output_key == "output_data"
        assert component._fail_on_error is False
    
    def test_initializes_with_defaults(self):
        """Test component uses defaults for optional config."""
        component = DataTransformationComponent({"pipeline_id": "test"})
        
        assert component._input_key == "data"
        assert component._output_key == "transformed_data"
        assert component._datasets_key == "datasets"
        assert component._fail_on_error is True
        assert component._convert_to_polars is True
    
    def test_initializes_without_config(self):
        """Test component initializes with no config."""
        component = DataTransformationComponent()
        
        assert component._pipeline_id is None
        assert component.config == {}


class TestDataTransformationComponentAddPipeline:
    """Tests for adding pipelines programmatically."""
    
    def test_add_pipeline(self):
        """Test adding a pipeline configuration."""
        component = DataTransformationComponent({"pipeline_id": "test"})
        
        pipeline_config = {
            "steps": [
                {"name": "select", "type": "select", "config": {"columns": ["a"]}}
            ]
        }
        
        component.add_pipeline("my_pipeline", pipeline_config)
        
        assert "my_pipeline" in component._additional_pipelines
    
    def test_add_pipeline_from_builder(self):
        """Test adding a pipeline from a Pipeline builder."""
        component = DataTransformationComponent({"pipeline_id": "builder_test"})
        
        pipeline = Pipeline("builder_test").select(["a", "b"]).filter("col('a') > 0")
        
        component.add_pipeline_from_builder(pipeline)
        
        assert "builder_test" in component._additional_pipelines


class TestDataTransformationComponentConversion:
    """Tests for data conversion methods."""
    
    def test_convert_dataframe_unchanged(self):
        """Test that DataFrames pass through unchanged."""
        component = DataTransformationComponent({"pipeline_id": "test"})
        df = pl.DataFrame({"a": [1, 2, 3]})
        
        result = component._convert_input_to_dataframe(df)
        
        assert result is df
    
    def test_convert_column_oriented_dict(self):
        """Test converting column-oriented dict."""
        component = DataTransformationComponent({"pipeline_id": "test"})
        data = {"a": [1, 2, 3], "b": [4, 5, 6]}
        
        result = component._convert_input_to_dataframe(data)
        
        assert isinstance(result, pl.DataFrame)
        assert result.columns == ["a", "b"]
        assert len(result) == 3
    
    def test_convert_single_record_dict(self):
        """Test converting single record dict."""
        component = DataTransformationComponent({"pipeline_id": "test"})
        data = {"a": 1, "b": 2}
        
        result = component._convert_input_to_dataframe(data)
        
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 1
    
    def test_convert_list_of_dicts(self):
        """Test converting list of dicts."""
        component = DataTransformationComponent({"pipeline_id": "test"})
        data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        
        result = component._convert_input_to_dataframe(data)
        
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 2
    
    def test_convert_empty_list(self):
        """Test converting empty list."""
        component = DataTransformationComponent({"pipeline_id": "test"})
        data = []
        
        result = component._convert_input_to_dataframe(data)
        
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 0
    
    def test_convert_invalid_type_raises(self):
        """Test that invalid type raises TypeError."""
        component = DataTransformationComponent({"pipeline_id": "test"})
        
        with pytest.raises(TypeError, match="Cannot convert data of type"):
            component._convert_input_to_dataframe("invalid")
    
    def test_convert_datasets(self):
        """Test converting multiple datasets."""
        component = DataTransformationComponent({"pipeline_id": "test"})
        datasets = {
            "df1": pl.DataFrame({"x": [1, 2]}),
            "df2": [{"y": 3}, {"y": 4}],
        }
        
        result = component._convert_datasets(datasets)
        
        assert "df1" in result
        assert "df2" in result
        assert isinstance(result["df2"], pl.DataFrame)


class TestDataTransformationComponentExecute:
    """Tests for execute method."""
    
    def test_execute_requires_pipeline_id(self):
        """Test that execute raises if pipeline_id not configured."""
        component = DataTransformationComponent({})
        
        with pytest.raises(ValueError, match="pipeline_id must be configured"):
            component.execute({"data": pl.DataFrame({"a": [1, 2, 3]})})
    
    def test_execute_handles_missing_input(self):
        """Test that execute handles missing input gracefully."""
        component = DataTransformationComponent({
            "pipeline_id": "test",
            "input_key": "missing_key",
        })
        
        context = {}
        result = component.execute(context)
        
        assert result["transformation_success"] is False
        assert "No input data" in result["transformation_result"]["error"]
    
    def test_execute_simple_transformation(self):
        """Test executing a simple transformation."""
        component = DataTransformationComponent({
            "pipeline_id": "test_select",
            "input_key": "data",
            "output_key": "result",
        })
        
        # Add pipeline
        component.add_pipeline("test_select", {
            "steps": [
                {"name": "select", "type": "select", "config": {"columns": ["a", "b"]}}
            ]
        })
        
        # Execute
        context = {
            "data": pl.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
        }
        result = component.execute(context)
        
        assert result["transformation_success"] is True
        assert "result" in result
        assert result["result"].columns == ["a", "b"]
    
    def test_execute_with_filter(self):
        """Test executing a transformation with filter."""
        component = DataTransformationComponent({
            "pipeline_id": "test_filter",
            "input_key": "data",
            "output_key": "result",
        })
        
        # Add pipeline
        component.add_pipeline("test_filter", {
            "steps": [
                {"name": "filter", "type": "filter", "config": {"condition": "col('value') > 20"}}
            ]
        })
        
        # Execute
        context = {
            "data": pl.DataFrame({"id": [1, 2, 3], "value": [10, 30, 50]})
        }
        result = component.execute(context)
        
        assert result["transformation_success"] is True
        assert len(result["result"]) == 2
    
    def test_execute_with_datasets(self):
        """Test executing a transformation with additional datasets."""
        component = DataTransformationComponent({
            "pipeline_id": "test_join",
            "input_key": "data",
            "output_key": "result",
            "datasets_key": "lookup_tables",
        })
        
        # Add pipeline with join
        component.add_pipeline("test_join", {
            "steps": [
                {
                    "name": "join", 
                    "type": "join", 
                    "config": {
                        "right_dataset": "categories",
                        "on": "category_id",
                        "how": "left"
                    }
                }
            ]
        })
        
        # Execute
        context = {
            "data": pl.DataFrame({"id": [1, 2], "category_id": [10, 20]}),
            "lookup_tables": {
                "categories": pl.DataFrame({
                    "category_id": [10, 20],
                    "category_name": ["A", "B"]
                })
            }
        }
        result = component.execute(context)
        
        assert result["transformation_success"] is True
        assert "category_name" in result["result"].columns
    
    def test_execute_converts_dict_input(self):
        """Test that dict input is converted to DataFrame."""
        component = DataTransformationComponent({
            "pipeline_id": "test",
            "input_key": "data",
            "output_key": "result",
        })
        
        component.add_pipeline("test", {
            "steps": [
                {"name": "select", "type": "select", "config": {"columns": ["a"]}}
            ]
        })
        
        context = {
            "data": {"a": [1, 2, 3], "b": [4, 5, 6]}
        }
        result = component.execute(context)
        
        assert result["transformation_success"] is True
        assert isinstance(result["result"], pl.DataFrame)
    
    def test_execute_stores_result_metadata(self):
        """Test that execution stores result metadata."""
        component = DataTransformationComponent({
            "pipeline_id": "test",
            "input_key": "data",
            "output_key": "result",
            "result_key": "metrics",
        })
        
        component.add_pipeline("test", {
            "steps": [
                {"name": "filter", "type": "filter", "config": {"condition": "col('a') > 1"}}
            ]
        })
        
        context = {
            "data": pl.DataFrame({"a": [1, 2, 3]})
        }
        result = component.execute(context)
        
        assert "metrics" in result
        assert result["metrics"]["pipeline_id"] == "test"
        assert result["metrics"]["success"] is True
        assert result["metrics"]["rows_in"] == 3
        assert result["metrics"]["rows_out"] == 2
        assert "execution_time_ms" in result["metrics"]
        assert "steps" in result["metrics"]


class TestDataTransformationComponentErrorHandling:
    """Tests for error handling."""
    
    def test_execute_fail_on_error_true_raises(self):
        """Test that fail_on_error=True raises exception."""
        component = DataTransformationComponent({
            "pipeline_id": "test",
            "input_key": "data",
            "fail_on_error": True,
        })
        
        component.add_pipeline("test", {
            "steps": [
                {"name": "select", "type": "select", "config": {"columns": ["nonexistent"]}}
            ]
        })
        
        context = {
            "data": pl.DataFrame({"a": [1, 2, 3]})
        }
        
        with pytest.raises(DataTransformationError):
            component.execute(context)
    
    def test_execute_fail_on_error_false_returns_failure(self):
        """Test that fail_on_error=False returns failure without raising."""
        component = DataTransformationComponent({
            "pipeline_id": "test",
            "input_key": "data",
            "fail_on_error": False,
        })
        
        component.add_pipeline("test", {
            "steps": [
                {"name": "select", "type": "select", "config": {"columns": ["nonexistent"]}}
            ]
        })
        
        context = {
            "data": pl.DataFrame({"a": [1, 2, 3]})
        }
        
        result = component.execute(context)
        
        assert result["transformation_success"] is False
        assert result["transformation_result"]["error_message"] is not None
    
    def test_execute_handles_conversion_error(self):
        """Test that conversion errors are handled gracefully."""
        component = DataTransformationComponent({
            "pipeline_id": "test",
            "input_key": "data",
            "fail_on_error": False,
        })
        
        context = {
            "data": "not a valid dataframe input"
        }
        
        result = component.execute(context)
        
        assert result["transformation_success"] is False
        assert "conversion error" in result["transformation_result"]["error"].lower()


class TestDataTransformationComponentWithPipelineBuilder:
    """Tests for using Pipeline builder with component."""
    
    def test_execute_with_pipeline_builder(self):
        """Test executing a transformation defined with Pipeline builder."""
        component = DataTransformationComponent({
            "pipeline_id": "builder_pipeline",
            "input_key": "data",
            "output_key": "result",
        })
        
        # Create pipeline with builder
        pipeline = (
            Pipeline("builder_pipeline")
            .filter("col('status') == 'active'")
            .select(["id", "name", "status"])
            .rename({"id": "user_id"})
        )
        
        component.add_pipeline_from_builder(pipeline)
        
        # Execute
        context = {
            "data": pl.DataFrame({
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "status": ["active", "inactive", "active"],
                "extra": ["x", "y", "z"]
            })
        }
        result = component.execute(context)
        
        assert result["transformation_success"] is True
        assert len(result["result"]) == 2
        assert result["result"].columns == ["user_id", "name", "status"]
