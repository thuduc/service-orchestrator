"""Tests for ExpressionParser."""

import pytest
import polars as pl

from frameworks.data_transformation.engine.expression_parser import ExpressionParser
from frameworks.data_transformation.exceptions import ExpressionParseError


@pytest.fixture
def parser():
    """Create an ExpressionParser instance."""
    return ExpressionParser()


@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing expressions."""
    return pl.DataFrame({
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35],
        "salary": [50000.0, 60000.0, 70000.0],
        "active": [True, False, True],
    })


class TestExpressionParserBasicParsing:
    """Tests for basic expression parsing."""
    
    def test_parse_column_reference(self, parser, sample_df):
        """Test parsing simple column reference."""
        expr = parser.parse("col('name')")
        
        result = sample_df.select(expr)
        assert result.columns == ["name"]
        assert result["name"].to_list() == ["Alice", "Bob", "Charlie"]
    
    def test_parse_column_double_quotes(self, parser, sample_df):
        """Test parsing column reference with double quotes."""
        expr = parser.parse('col("age")')
        
        result = sample_df.select(expr)
        assert result.columns == ["age"]
    
    def test_parse_literal_integer(self, parser, sample_df):
        """Test parsing literal integer."""
        expr = parser.parse("lit(42)")
        
        result = sample_df.select(expr.alias("constant"))
        assert result["constant"][0] == 42
    
    def test_parse_literal_string(self, parser, sample_df):
        """Test parsing literal string."""
        expr = parser.parse("lit('hello')")
        
        result = sample_df.select(expr.alias("greeting"))
        assert result["greeting"][0] == "hello"
    
    def test_parse_literal_float(self, parser, sample_df):
        """Test parsing literal float."""
        expr = parser.parse("lit(3.14)")
        
        result = sample_df.select(expr.alias("pi"))
        assert abs(result["pi"][0] - 3.14) < 0.001


class TestExpressionParserArithmetic:
    """Tests for arithmetic expression parsing."""
    
    def test_parse_addition(self, parser, sample_df):
        """Test parsing addition expression."""
        expr = parser.parse("col('age') + 10")
        
        result = sample_df.select(expr.alias("age_plus_10"))
        assert result["age_plus_10"].to_list() == [35, 40, 45]
    
    def test_parse_subtraction(self, parser, sample_df):
        """Test parsing subtraction expression."""
        expr = parser.parse("col('age') - 5")
        
        result = sample_df.select(expr.alias("age_minus_5"))
        assert result["age_minus_5"].to_list() == [20, 25, 30]
    
    def test_parse_multiplication(self, parser, sample_df):
        """Test parsing multiplication expression."""
        expr = parser.parse("col('salary') * 1.1")
        
        result = sample_df.select(expr.alias("raised_salary"))
        assert abs(result["raised_salary"][0] - 55000.0) < 0.01
    
    def test_parse_division(self, parser, sample_df):
        """Test parsing division expression."""
        expr = parser.parse("col('salary') / 1000")
        
        result = sample_df.select(expr.alias("salary_k"))
        assert result["salary_k"].to_list() == [50.0, 60.0, 70.0]
    
    def test_parse_complex_arithmetic(self, parser, sample_df):
        """Test parsing complex arithmetic expression."""
        expr = parser.parse("(col('salary') + 10000) / 12")
        
        result = sample_df.select(expr.alias("monthly"))
        assert abs(result["monthly"][0] - 5000.0) < 0.01


class TestExpressionParserComparisons:
    """Tests for comparison expression parsing."""
    
    def test_parse_equals(self, parser, sample_df):
        """Test parsing equals comparison."""
        expr = parser.parse("col('name') == 'Alice'")
        
        result = sample_df.filter(expr)
        assert len(result) == 1
        assert result["name"][0] == "Alice"
    
    def test_parse_not_equals(self, parser, sample_df):
        """Test parsing not equals comparison."""
        expr = parser.parse("col('name') != 'Alice'")
        
        result = sample_df.filter(expr)
        assert len(result) == 2
    
    def test_parse_greater_than(self, parser, sample_df):
        """Test parsing greater than comparison."""
        expr = parser.parse("col('age') > 25")
        
        result = sample_df.filter(expr)
        assert len(result) == 2
        assert 25 not in result["age"].to_list()
    
    def test_parse_greater_than_or_equal(self, parser, sample_df):
        """Test parsing greater than or equal comparison."""
        expr = parser.parse("col('age') >= 25")
        
        result = sample_df.filter(expr)
        assert len(result) == 3
    
    def test_parse_less_than(self, parser, sample_df):
        """Test parsing less than comparison."""
        expr = parser.parse("col('age') < 30")
        
        result = sample_df.filter(expr)
        assert len(result) == 1
        assert result["age"][0] == 25
    
    def test_parse_less_than_or_equal(self, parser, sample_df):
        """Test parsing less than or equal comparison."""
        expr = parser.parse("col('age') <= 30")
        
        result = sample_df.filter(expr)
        assert len(result) == 2


class TestExpressionParserLogical:
    """Tests for logical expression parsing."""
    
    def test_parse_and_operator(self, parser, sample_df):
        """Test parsing AND operator."""
        expr = parser.parse("(col('age') > 25) & (col('active') == True)")
        
        result = sample_df.filter(expr)
        assert len(result) == 1
        assert result["name"][0] == "Charlie"
    
    def test_parse_or_operator(self, parser, sample_df):
        """Test parsing OR operator."""
        expr = parser.parse("(col('age') < 30) | (col('salary') > 65000)")
        
        result = sample_df.filter(expr)
        assert len(result) == 2  # Alice (age < 30) and Charlie (salary > 65000)


class TestExpressionParserMethods:
    """Tests for method call parsing."""
    
    def test_parse_is_null(self, parser):
        """Test parsing is_null method."""
        df = pl.DataFrame({
            "value": [1, None, 3]
        })
        
        expr = parser.parse("col('value').is_null()")
        
        result = df.filter(expr)
        assert len(result) == 1
    
    def test_parse_is_not_null(self, parser):
        """Test parsing is_not_null method."""
        df = pl.DataFrame({
            "value": [1, None, 3]
        })
        
        expr = parser.parse("col('value').is_not_null()")
        
        result = df.filter(expr)
        assert len(result) == 2
    
    def test_parse_alias(self, parser, sample_df):
        """Test parsing alias method."""
        expr = parser.parse("col('name').alias('customer_name')")
        
        result = sample_df.select(expr)
        assert result.columns == ["customer_name"]
    
    def test_parse_sum_aggregation(self, parser, sample_df):
        """Test parsing sum aggregation method."""
        expr = parser.parse("col('salary').sum()")
        
        result = sample_df.select(expr)
        assert result["salary"][0] == 180000.0
    
    def test_parse_mean_aggregation(self, parser, sample_df):
        """Test parsing mean aggregation method."""
        expr = parser.parse("col('age').mean()")
        
        result = sample_df.select(expr)
        assert result["age"][0] == 30.0


class TestExpressionParserStringMethods:
    """Tests for string method parsing."""
    
    def test_parse_str_contains(self, parser):
        """Test parsing str.contains method."""
        df = pl.DataFrame({
            "text": ["hello world", "goodbye", "hello there"]
        })
        
        expr = parser.parse("col('text').str.contains('hello')")
        
        result = df.filter(expr)
        assert len(result) == 2
    
    def test_parse_str_starts_with(self, parser):
        """Test parsing str.starts_with method."""
        df = pl.DataFrame({
            "text": ["hello world", "goodbye", "hello there"]
        })
        
        expr = parser.parse("col('text').str.starts_with('hello')")
        
        result = df.filter(expr)
        assert len(result) == 2
    
    def test_parse_str_to_lowercase(self, parser):
        """Test parsing str.to_lowercase method."""
        df = pl.DataFrame({
            "text": ["HELLO", "World", "TEST"]
        })
        
        expr = parser.parse("col('text').str.to_lowercase()")
        
        result = df.select(expr)
        assert result["text"].to_list() == ["hello", "world", "test"]


class TestExpressionParserValidation:
    """Tests for expression validation."""
    
    def test_validate_valid_expression(self, parser):
        """Test validating a valid expression."""
        error = parser.validate("col('name')")
        
        assert error is None
    
    def test_validate_invalid_syntax(self, parser):
        """Test validating expression with syntax error."""
        error = parser.validate("col('name'")  # Missing closing paren
        
        assert error is not None
        assert "syntax" in error.lower() or "error" in error.lower()
    
    def test_validate_empty_expression(self, parser):
        """Test validating empty expression."""
        error = parser.validate("")
        
        assert error is not None
    
    def test_validate_whitespace_only(self, parser):
        """Test validating whitespace-only expression."""
        error = parser.validate("   ")
        
        assert error is not None


class TestExpressionParserSecurity:
    """Tests for expression parser security features."""
    
    def test_rejects_disallowed_function(self, parser):
        """Test that disallowed functions are rejected."""
        with pytest.raises(ExpressionParseError) as exc_info:
            parser.parse("eval('malicious code')")
        
        assert "not allowed" in str(exc_info.value).lower() or "not implemented" in str(exc_info.value).lower()
    
    def test_rejects_import(self, parser):
        """Test that import statements are rejected."""
        with pytest.raises(ExpressionParseError):
            parser.parse("__import__('os')")
    
    def test_rejects_unknown_name(self, parser):
        """Test that unknown names are rejected."""
        with pytest.raises(ExpressionParseError) as exc_info:
            parser.parse("unknown_variable")
        
        assert "unknown" in str(exc_info.value).lower()


class TestExpressionParserEdgeCases:
    """Tests for edge cases in expression parsing."""
    
    def test_parse_nested_method_calls(self, parser, sample_df):
        """Test parsing nested method calls."""
        expr = parser.parse("col('salary').sum().alias('total')")
        
        # This should work - chained calls
        result = sample_df.select(expr)
        assert "total" in result.columns
    
    def test_parse_expression_with_list(self, parser, sample_df):
        """Test parsing expression containing a list."""
        # coalesce takes multiple arguments
        df = pl.DataFrame({
            "a": [None, 2, None],
            "b": [1, None, 3],
        })
        
        expr = parser.parse("coalesce(col('a'), col('b'))")
        
        result = df.select(expr.alias("result"))
        assert result["result"].to_list() == [1, 2, 3]
    
    def test_parse_boolean_true(self, parser, sample_df):
        """Test parsing True literal."""
        expr = parser.parse("col('active') == True")
        
        result = sample_df.filter(expr)
        assert len(result) == 2
    
    def test_parse_boolean_false(self, parser, sample_df):
        """Test parsing False literal."""
        expr = parser.parse("col('active') == False")
        
        result = sample_df.filter(expr)
        assert len(result) == 1
        assert result["name"][0] == "Bob"
    
    def test_parse_column_multiply_two_columns(self, parser, sample_df):
        """Test parsing expression with two columns."""
        expr = parser.parse("col('age') * col('salary')")
        
        result = sample_df.select(expr.alias("product"))
        assert result["product"][0] == 25 * 50000.0


class TestExpressionParserWhenThenOtherwise:
    """Tests for when/then/otherwise conditional parsing."""
    
    def test_parse_when_then_otherwise(self, parser, sample_df):
        """Test parsing when/then/otherwise expression."""
        # The expression parser can parse this, but executing requires proper handling
        # Test that it parses without error
        expr = parser.parse(
            "when(col('age') >= 30).then(lit('senior')).otherwise(lit('junior'))"
        )
        
        result = sample_df.select(expr.alias("category"))
        assert result["category"].to_list() == ["junior", "senior", "senior"]
    
    def test_parse_when_then_chained(self, parser, sample_df):
        """Test parsing chained when/then expressions."""
        # Note: Polars when().then().when().then().otherwise() pattern
        expr = parser.parse(
            "when(col('age') >= 35).then(lit('senior'))"
            ".when(col('age') >= 30).then(lit('mid'))"
            ".otherwise(lit('junior'))"
        )
        
        result = sample_df.select(expr.alias("level"))
        assert result["level"].to_list() == ["junior", "mid", "senior"]
