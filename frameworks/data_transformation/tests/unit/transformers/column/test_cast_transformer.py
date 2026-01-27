"""Tests for CastTransformer."""

import pytest
import polars as pl

from frameworks.data_transformation.transformers.column.cast import CastTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext
from frameworks.data_transformation.exceptions import ConfigurationError


class TestCastTransformer:
    """Tests for CastTransformer."""
    
    @pytest.fixture
    def numeric_df(self):
        """DataFrame with numeric values for testing casts."""
        return pl.DataFrame({
            "int_col": [1, 2, 3, 4, 5],
            "float_col": [1.1, 2.2, 3.3, 4.4, 5.5],
            "str_col": ["1", "2", "3", "4", "5"],
            "bool_col": [True, False, True, False, True],
        })
    
    def test_cast_int_to_float(self, numeric_df, empty_context):
        """Test casting integer to float."""
        context = TransformationContext(data=numeric_df)
        transformer = CastTransformer(
            name="cast_to_float",
            config={"schema": {"int_col": "Float64"}}
        )
        
        result = transformer.transform(numeric_df, context)
        
        assert result["int_col"].dtype == pl.Float64
    
    def test_cast_float_to_int(self, numeric_df, empty_context):
        """Test casting float to integer."""
        context = TransformationContext(data=numeric_df)
        transformer = CastTransformer(
            name="cast_to_int",
            config={"schema": {"float_col": "Int64"}}
        )
        
        result = transformer.transform(numeric_df, context)
        
        assert result["float_col"].dtype == pl.Int64
    
    def test_cast_string_to_int(self, numeric_df, empty_context):
        """Test casting string to integer."""
        context = TransformationContext(data=numeric_df)
        transformer = CastTransformer(
            name="cast_str_to_int",
            config={"schema": {"str_col": "Int64"}}
        )
        
        result = transformer.transform(numeric_df, context)
        
        assert result["str_col"].dtype == pl.Int64
        assert result["str_col"].to_list() == [1, 2, 3, 4, 5]
    
    def test_cast_multiple_columns(self, numeric_df, empty_context):
        """Test casting multiple columns at once."""
        context = TransformationContext(data=numeric_df)
        transformer = CastTransformer(
            name="cast_multiple",
            config={"schema": {
                "int_col": "Float32",
                "float_col": "Int32",
            }}
        )
        
        result = transformer.transform(numeric_df, context)
        
        assert result["int_col"].dtype == pl.Float32
        assert result["float_col"].dtype == pl.Int32
    
    def test_cast_to_string(self, numeric_df, empty_context):
        """Test casting to string (Utf8)."""
        context = TransformationContext(data=numeric_df)
        transformer = CastTransformer(
            name="cast_to_str",
            config={"schema": {"int_col": "Utf8"}}
        )
        
        result = transformer.transform(numeric_df, context)
        
        assert result["int_col"].dtype == pl.Utf8
        assert result["int_col"].to_list() == ["1", "2", "3", "4", "5"]
    
    def test_cast_to_boolean(self, empty_context):
        """Test casting to boolean."""
        df = pl.DataFrame({"values": [0, 1, 0, 1, 1]})
        context = TransformationContext(data=df)
        transformer = CastTransformer(
            name="cast_to_bool",
            config={"schema": {"values": "Boolean"}}
        )
        
        result = transformer.transform(df, context)
        
        assert result["values"].dtype == pl.Boolean
        assert result["values"].to_list() == [False, True, False, True, True]
    
    def test_cast_all_integer_types(self, empty_context):
        """Test casting to various integer types."""
        df = pl.DataFrame({"value": [1, 2, 3]})
        context = TransformationContext(data=df)
        
        int_types = ["Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64"]
        
        for int_type in int_types:
            transformer = CastTransformer(
                name=f"cast_to_{int_type}",
                config={"schema": {"value": int_type}}
            )
            
            result = transformer.transform(df, context)
            expected_dtype = getattr(pl, int_type)
            assert result["value"].dtype == expected_dtype
    
    def test_cast_all_float_types(self, empty_context):
        """Test casting to various float types."""
        df = pl.DataFrame({"value": [1, 2, 3]})
        context = TransformationContext(data=df)
        
        float_types = ["Float32", "Float64"]
        
        for float_type in float_types:
            transformer = CastTransformer(
                name=f"cast_to_{float_type}",
                config={"schema": {"value": float_type}}
            )
            
            result = transformer.transform(df, context)
            expected_dtype = getattr(pl, float_type)
            assert result["value"].dtype == expected_dtype
    
    def test_cast_string_alias(self, numeric_df, empty_context):
        """Test 'String' alias for Utf8."""
        context = TransformationContext(data=numeric_df)
        transformer = CastTransformer(
            name="cast_to_string",
            config={"schema": {"int_col": "String"}}
        )
        
        result = transformer.transform(numeric_df, context)
        
        assert result["int_col"].dtype == pl.Utf8
    
    def test_cast_bool_alias(self, empty_context):
        """Test 'Bool' alias for Boolean."""
        df = pl.DataFrame({"values": [0, 1, 1]})
        context = TransformationContext(data=df)
        transformer = CastTransformer(
            name="cast_to_bool",
            config={"schema": {"values": "Bool"}}
        )
        
        result = transformer.transform(df, context)
        
        assert result["values"].dtype == pl.Boolean
    
    def test_cast_unknown_type_raises_error(self, numeric_df, empty_context):
        """Test that unknown type raises error."""
        context = TransformationContext(data=numeric_df)
        transformer = CastTransformer(
            name="cast_unknown",
            config={"schema": {"int_col": "UnknownType"}}
        )
        
        with pytest.raises(ConfigurationError) as exc_info:
            transformer.transform(numeric_df, context)
        
        assert "UnknownType" in str(exc_info.value)
    
    def test_validate_config_missing_schema(self):
        """Test config validation catches missing schema."""
        transformer = CastTransformer(name="test", config={})
        
        error = transformer.validate_config({})
        
        assert error is not None
        assert "schema" in error.lower()
    
    def test_validate_config_schema_not_dict(self):
        """Test config validation catches non-dict schema."""
        transformer = CastTransformer(
            name="test", 
            config={"schema": ["Int64"]}
        )
        
        error = transformer.validate_config({"schema": ["Int64"]})
        
        assert error is not None
        assert "dictionary" in error.lower()
    
    def test_validate_config_unknown_type(self):
        """Test config validation catches unknown type."""
        config = {"schema": {"col": "BadType"}}
        transformer = CastTransformer(name="test", config=config)
        
        error = transformer.validate_config(config)
        
        assert error is not None
        assert "BadType" in error
    
    def test_validate_config_valid(self):
        """Test config validation passes with valid config."""
        config = {"schema": {"col": "Int64"}}
        transformer = CastTransformer(name="test", config=config)
        
        error = transformer.validate_config(config)
        
        assert error is None
    
    def test_transformer_type(self):
        """Test transformer_type property."""
        transformer = CastTransformer(
            name="test", 
            config={"schema": {"a": "Int64"}}
        )
        
        assert transformer.transformer_type == "cast"
    
    def test_cast_preserves_non_casted_columns(self, numeric_df, empty_context):
        """Test that non-casted columns are preserved."""
        context = TransformationContext(data=numeric_df)
        transformer = CastTransformer(
            name="cast_one",
            config={"schema": {"int_col": "Float64"}}
        )
        
        result = transformer.transform(numeric_df, context)
        
        # Other columns should be unchanged
        assert result["float_col"].dtype == numeric_df["float_col"].dtype
        assert result["str_col"].dtype == numeric_df["str_col"].dtype
        assert result["bool_col"].dtype == numeric_df["bool_col"].dtype
