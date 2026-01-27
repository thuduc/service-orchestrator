"""Tests for Pipeline fluent builder."""

import pytest
import polars as pl
from polars.testing import assert_frame_equal

from frameworks.data_transformation.engine.pipeline_builder import Pipeline


class TestPipelineInit:
    """Tests for Pipeline initialization."""
    
    def test_pipeline_initializes_with_id(self):
        """Test pipeline initializes with just an ID."""
        pipeline = Pipeline("test_pipeline")
        
        assert pipeline.pipeline_id == "test_pipeline"
        assert pipeline.steps == []
    
    def test_pipeline_initializes_with_description(self):
        """Test pipeline initializes with ID and description."""
        pipeline = Pipeline("test_pipeline", description="Test description")
        
        config = pipeline.to_config()
        assert config["description"] == "Test description"
    
    def test_pipeline_repr(self):
        """Test string representation."""
        pipeline = Pipeline("test_pipeline").select(["a"]).filter("col('a') > 0")
        
        assert "test_pipeline" in repr(pipeline)
        assert "2" in repr(pipeline)  # 2 steps


class TestPipelineColumnOperations:
    """Tests for column operations."""
    
    def test_select(self):
        """Test select operation."""
        df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
        
        result = Pipeline("test").select(["a", "b"]).execute(df)
        
        assert result.success
        assert result.data.columns == ["a", "b"]
    
    def test_drop(self):
        """Test drop operation."""
        df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
        
        result = Pipeline("test").drop(["c"]).execute(df)
        
        assert result.success
        assert "c" not in result.data.columns
        assert "a" in result.data.columns
    
    def test_rename(self):
        """Test rename operation."""
        df = pl.DataFrame({"old_name": [1, 2, 3]})
        
        result = Pipeline("test").rename({"old_name": "new_name"}).execute(df)
        
        assert result.success
        assert "new_name" in result.data.columns
        assert "old_name" not in result.data.columns
    
    def test_cast(self):
        """Test cast operation."""
        df = pl.DataFrame({"int_col": [1, 2, 3]})
        
        result = Pipeline("test").cast({"int_col": "Float64"}).execute(df)
        
        assert result.success
        assert result.data["int_col"].dtype == pl.Float64
    
    def test_with_columns(self):
        """Test with_columns operation."""
        df = pl.DataFrame({"a": [1, 2, 3], "b": [10, 20, 30]})
        
        result = (
            Pipeline("test")
            .with_columns({"c": "col('a') + col('b')"})
            .execute(df)
        )
        
        assert result.success
        assert "c" in result.data.columns
        assert result.data["c"].to_list() == [11, 22, 33]


class TestPipelineRowOperations:
    """Tests for row operations."""
    
    def test_filter(self):
        """Test filter operation."""
        df = pl.DataFrame({"value": [10, 20, 30, 40, 50]})
        
        result = Pipeline("test").filter("col('value') > 25").execute(df)
        
        assert result.success
        assert len(result.data) == 3
        assert result.data["value"].to_list() == [30, 40, 50]
    
    def test_sort(self):
        """Test sort operation."""
        df = pl.DataFrame({"value": [3, 1, 2]})
        
        result = Pipeline("test").sort(by=["value"]).execute(df)
        
        assert result.success
        assert result.data["value"].to_list() == [1, 2, 3]
    
    def test_sort_descending(self):
        """Test sort descending."""
        df = pl.DataFrame({"value": [1, 3, 2]})
        
        result = Pipeline("test").sort(by=["value"], descending=True).execute(df)
        
        assert result.success
        assert result.data["value"].to_list() == [3, 2, 1]
    
    def test_unique(self):
        """Test unique operation."""
        df = pl.DataFrame({"value": [1, 2, 2, 3, 3, 3]})
        
        result = Pipeline("test").unique(subset=["value"]).execute(df)
        
        assert result.success
        assert len(result.data) == 3
    
    def test_head(self):
        """Test head operation."""
        df = pl.DataFrame({"value": list(range(10))})
        
        result = Pipeline("test").head(n=3).execute(df)
        
        assert result.success
        assert len(result.data) == 3
    
    def test_tail(self):
        """Test tail operation."""
        df = pl.DataFrame({"value": list(range(10))})
        
        result = Pipeline("test").tail(n=3).execute(df)
        
        assert result.success
        assert len(result.data) == 3
        assert result.data["value"].to_list() == [7, 8, 9]
    
    def test_slice(self):
        """Test slice operation."""
        df = pl.DataFrame({"value": list(range(10))})
        
        result = Pipeline("test").slice(offset=2, length=3).execute(df)
        
        assert result.success
        assert len(result.data) == 3
        assert result.data["value"].to_list() == [2, 3, 4]
    
    def test_sample(self):
        """Test sample operation."""
        df = pl.DataFrame({"value": list(range(100))})
        
        result = Pipeline("test").sample(n=10, seed=42).execute(df)
        
        assert result.success
        assert len(result.data) == 10
    
    def test_sample_fraction(self):
        """Test sample with fraction."""
        df = pl.DataFrame({"value": list(range(100))})
        
        result = Pipeline("test").sample(fraction=0.1, seed=42).execute(df)
        
        assert result.success
        assert len(result.data) == 10  # 10% of 100
    
    def test_drop_nulls(self):
        """Test drop_nulls operation."""
        df = pl.DataFrame({"value": [1, None, 3, None, 5]})
        
        result = Pipeline("test").drop_nulls().execute(df)
        
        assert result.success
        assert len(result.data) == 3
        assert result.data["value"].to_list() == [1, 3, 5]


class TestPipelineReshapeOperations:
    """Tests for reshape operations."""
    
    def test_pivot(self):
        """Test pivot operation."""
        df = pl.DataFrame({
            "category": ["A", "A", "B", "B"],
            "type": ["X", "Y", "X", "Y"],
            "value": [1, 2, 3, 4]
        })
        
        result = (
            Pipeline("test")
            .pivot(on="type", index="category", values="value", aggregate_function="first")
            .execute(df)
        )
        
        assert result.success
        assert "X" in result.data.columns
        assert "Y" in result.data.columns
    
    def test_unpivot(self):
        """Test unpivot operation."""
        df = pl.DataFrame({
            "id": [1, 2],
            "col_a": [10, 20],
            "col_b": [30, 40]
        })
        
        result = (
            Pipeline("test")
            .unpivot(on=["col_a", "col_b"], index=["id"])
            .execute(df)
        )
        
        assert result.success
        assert "variable" in result.data.columns
        assert "value" in result.data.columns
        assert len(result.data) == 4  # 2 rows x 2 columns
    
    def test_explode(self):
        """Test explode operation."""
        df = pl.DataFrame({
            "id": [1, 2],
            "values": [[1, 2, 3], [4, 5]]
        })
        
        result = Pipeline("test").explode(columns=["values"]).execute(df)
        
        assert result.success
        assert len(result.data) == 5  # 3 + 2 values


class TestPipelineAggregateOperations:
    """Tests for aggregate operations."""
    
    def test_group_by_sum(self):
        """Test group_by with sum aggregation."""
        df = pl.DataFrame({
            "category": ["A", "A", "B", "B"],
            "value": [10, 20, 30, 40]
        })
        
        result = (
            Pipeline("test")
            .group_by(by=["category"], agg={"total": {"column": "value", "agg": "sum"}})
            .execute(df)
        )
        
        assert result.success
        assert len(result.data) == 2
        
        # Check totals (order may vary)
        totals = dict(zip(result.data["category"].to_list(), result.data["total"].to_list()))
        assert totals["A"] == 30
        assert totals["B"] == 70
    
    def test_group_by_multiple_aggs(self):
        """Test group_by with multiple aggregations."""
        df = pl.DataFrame({
            "category": ["A", "A", "B", "B"],
            "value": [10, 20, 30, 40]
        })
        
        result = (
            Pipeline("test")
            .group_by(
                by=["category"],
                agg={
                    "total": {"column": "value", "agg": "sum"},
                    "avg": {"column": "value", "agg": "mean"},
                    "cnt": {"column": "value", "agg": "count"}
                }
            )
            .execute(df)
        )
        
        assert result.success
        assert "total" in result.data.columns
        assert "avg" in result.data.columns
        assert "cnt" in result.data.columns


class TestPipelineCombineOperations:
    """Tests for combine operations."""
    
    def test_join(self):
        """Test join operation."""
        left = pl.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        right = pl.DataFrame({"id": [1, 2], "other": ["x", "y"]})
        
        result = (
            Pipeline("test")
            .join(right="other_df", on="id", how="left")
            .execute(left, datasets={"other_df": right})
        )
        
        assert result.success
        assert "other" in result.data.columns
        assert len(result.data) == 3
    
    def test_join_inner(self):
        """Test inner join."""
        left = pl.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        right = pl.DataFrame({"id": [1, 2], "other": ["x", "y"]})
        
        result = (
            Pipeline("test")
            .join(right="other_df", on="id", how="inner")
            .execute(left, datasets={"other_df": right})
        )
        
        assert result.success
        assert len(result.data) == 2  # Only matching rows
    
    def test_concat(self):
        """Test concat operation."""
        df1 = pl.DataFrame({"id": [1, 2], "value": [10, 20]})
        df2 = pl.DataFrame({"id": [3, 4], "value": [30, 40]})
        
        result = (
            Pipeline("test")
            .concat(datasets=["other"])
            .execute(df1, datasets={"other": df2})
        )
        
        assert result.success
        assert len(result.data) == 4


class TestPipelineFillOperations:
    """Tests for fill operations."""
    
    def test_fill_null_with_value(self):
        """Test fill_null with a literal value."""
        df = pl.DataFrame({"value": [1, None, 3, None, 5]})
        
        result = Pipeline("test").fill_null(value=0).execute(df)
        
        assert result.success
        assert result.data["value"].to_list() == [1, 0, 3, 0, 5]
    
    def test_fill_null_with_strategy(self):
        """Test fill_null with forward fill strategy."""
        df = pl.DataFrame({"value": [1, None, None, 4, None]})
        
        result = Pipeline("test").fill_null(strategy="forward").execute(df)
        
        assert result.success
        assert result.data["value"].to_list() == [1, 1, 1, 4, 4]
    
    def test_fill_nan(self):
        """Test fill_nan operation."""
        df = pl.DataFrame({"value": [1.0, float("nan"), 3.0]})
        
        result = Pipeline("test").fill_nan(value=0.0).execute(df)
        
        assert result.success
        assert result.data["value"].to_list() == [1.0, 0.0, 3.0]


class TestPipelineChaining:
    """Tests for method chaining and complex pipelines."""
    
    def test_chain_multiple_operations(self):
        """Test chaining multiple operations together."""
        df = pl.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "category": ["A", "A", "B", "B", "C"],
            "value": [10, 20, 30, 40, 50]
        })
        
        result = (
            Pipeline("complex_pipeline")
            .filter("col('category') != 'C'")
            .with_columns({"doubled": "col('value') * 2"})
            .select(["id", "category", "value", "doubled"])
            .sort(by=["value"], descending=True)
            .head(n=3)
            .execute(df)
        )
        
        assert result.success
        assert len(result.data) == 3
        assert result.data.columns == ["id", "category", "value", "doubled"]
        # Should be sorted descending by value, top 3 from categories A and B
        assert result.data["value"].to_list() == [40, 30, 20]
    
    def test_pipeline_preserves_order_of_steps(self):
        """Test that steps execute in order."""
        pipeline = (
            Pipeline("test")
            .select(["a"])
            .filter("col('a') > 0")
            .rename({"a": "b"})
        )
        
        steps = pipeline.steps
        assert len(steps) == 3
        assert steps[0]["type"] == "select"
        assert steps[1]["type"] == "filter"
        assert steps[2]["type"] == "rename"


class TestPipelineToConfig:
    """Tests for to_config method."""
    
    def test_to_config_empty_pipeline(self):
        """Test to_config for empty pipeline."""
        pipeline = Pipeline("test", description="Empty pipeline")
        
        config = pipeline.to_config()
        
        assert config["description"] == "Empty pipeline"
        assert config["steps"] == []
    
    def test_to_config_with_steps(self):
        """Test to_config includes all steps."""
        pipeline = (
            Pipeline("test")
            .select(["a", "b"])
            .filter("col('a') > 0")
        )
        
        config = pipeline.to_config()
        
        assert len(config["steps"]) == 2
        assert config["steps"][0]["type"] == "select"
        assert config["steps"][0]["config"]["columns"] == ["a", "b"]
        assert config["steps"][1]["type"] == "filter"
        assert config["steps"][1]["config"]["condition"] == "col('a') > 0"
    
    def test_to_config_is_serializable(self):
        """Test that config can be serialized to JSON."""
        import json
        
        pipeline = (
            Pipeline("test", description="Test pipeline")
            .select(["a"])
            .filter("col('a') > 0")
            .rename({"a": "b"})
        )
        
        config = pipeline.to_config()
        
        # Should not raise
        json_str = json.dumps(config)
        parsed = json.loads(json_str)
        
        assert parsed["steps"] == config["steps"]


class TestPipelineExecution:
    """Tests for execute method."""
    
    def test_execute_returns_transformation_result(self):
        """Test that execute returns proper TransformationResult."""
        df = pl.DataFrame({"a": [1, 2, 3]})
        
        result = Pipeline("test").select(["a"]).execute(df)
        
        assert result.success
        assert result.pipeline_id == "test"
        assert isinstance(result.data, pl.DataFrame)
        assert result.rows_in == 3
        assert result.rows_out == 3
    
    def test_execute_records_step_results(self):
        """Test that execute records step results."""
        df = pl.DataFrame({"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]})
        
        result = (
            Pipeline("test")
            .filter("col('a') > 2")
            .select(["a"])
            .execute(df)
        )
        
        assert result.success
        assert len(result.step_results) == 2
        assert result.step_results[0].step_name.startswith("filter")
        assert result.step_results[1].step_name.startswith("select")
    
    def test_execute_handles_empty_result(self):
        """Test execution when filter removes all rows."""
        df = pl.DataFrame({"value": [1, 2, 3]})
        
        result = Pipeline("test").filter("col('value') > 100").execute(df)
        
        assert result.success
        assert len(result.data) == 0
    
    def test_execute_does_not_modify_input(self):
        """Test that execute does not modify the input DataFrame."""
        df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        original_columns = df.columns.copy()
        original_len = len(df)
        
        _ = Pipeline("test").drop(["b"]).filter("col('a') > 1").execute(df)
        
        assert df.columns == original_columns
        assert len(df) == original_len


class TestPipelineErrorHandling:
    """Tests for error handling."""
    
    def test_invalid_filter_expression(self):
        """Test that invalid filter expression causes failure."""
        df = pl.DataFrame({"value": [1, 2, 3]})
        
        result = Pipeline("test").filter("invalid_syntax!!!").execute(df)
        
        assert not result.success
        assert result.error_message is not None
    
    def test_select_missing_column(self):
        """Test that selecting non-existent column raises error."""
        df = pl.DataFrame({"a": [1, 2, 3]})
        
        result = Pipeline("test").select(["nonexistent"]).execute(df)
        
        assert not result.success
    
    def test_join_missing_dataset(self):
        """Test that join with missing dataset returns failure result."""
        df = pl.DataFrame({"id": [1, 2, 3]})
        
        result = Pipeline("test").join(right="missing", on="id").execute(df)
        
        # Returns a failure result rather than raising
        assert not result.success
        assert result.error_message is not None


class TestPipelineStepNaming:
    """Tests for automatic step naming."""
    
    def test_step_names_are_unique(self):
        """Test that multiple steps of same type get unique names."""
        pipeline = (
            Pipeline("test")
            .filter("col('a') > 0")
            .filter("col('b') > 0")
            .filter("col('c') > 0")
        )
        
        names = [step["name"] for step in pipeline.steps]
        assert len(names) == len(set(names))  # All unique
    
    def test_step_names_include_type(self):
        """Test that step names include the transformer type."""
        pipeline = Pipeline("test").select(["a"]).filter("col('a') > 0")
        
        assert "select" in pipeline.steps[0]["name"]
        assert "filter" in pipeline.steps[1]["name"]
