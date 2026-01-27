"""ExpressionParser - Parse string expressions into Polars expressions."""

import ast
import operator
from typing import Any, Dict, Optional

import polars as pl

from frameworks.data_transformation.exceptions import ExpressionParseError


class ExpressionParser:
    """
    Parse string expressions into Polars expressions.
    
    Supports a safe subset of Polars expression syntax for use in
    configuration files. Expressions are parsed and validated before
    execution to prevent code injection.
    
    Supported syntax:
        - Column references: col("name"), col('name')
        - Literals: 123, 3.14, "string", True, False, None
        - Arithmetic: +, -, *, /, //, %, **
        - Comparison: ==, !=, <, <=, >, >=
        - Logical: &, |, ~
        - Methods: .is_null(), .is_not_null(), .str.contains(), etc.
        - Functions: sum(), mean(), count(), min(), max(), etc.
        - Conditionals: when(...).then(...).otherwise(...)
        - Aggregations: col("x").sum(), col("x").mean(), etc.
    
    Example:
        parser = ExpressionParser()
        
        # Simple column reference
        expr = parser.parse("col('age')")
        
        # Computed expression
        expr = parser.parse("col('price') * col('quantity')")
        
        # Conditional
        expr = parser.parse(
            "when(col('age') >= 18).then('adult').otherwise('minor')"
        )
    """
    
    # Allowed function names (whitelist for security)
    ALLOWED_FUNCTIONS = {
        # Column reference
        "col", "lit",
        # Aggregations
        "sum", "mean", "avg", "min", "max", "count", "first", "last",
        "std", "var", "median", "quantile", "n_unique",
        # Conditionals
        "when", "then", "otherwise",
        # Type conversion
        "cast",
        # Null handling
        "coalesce", "fill_null",
        # String functions
        "concat", "concat_str",
        # Math functions
        "abs", "sqrt", "exp", "log", "log10",
        "ceil", "floor", "round",
        # Date functions
        "date", "datetime", "duration",
    }
    
    # Allowed methods on expressions
    ALLOWED_METHODS = {
        # Null checks
        "is_null", "is_not_null", "is_nan", "is_not_nan",
        "fill_null", "fill_nan",
        # String methods
        "str", "contains", "starts_with", "ends_with",
        "to_lowercase", "to_uppercase", "strip", "replace",
        "len_chars", "slice", "strftime",
        # Numeric methods
        "abs", "sqrt", "log", "exp", "round", "floor", "ceil",
        # Date methods
        "dt", "year", "month", "day", "hour", "minute", "second",
        "date", "time", "timestamp",
        # Aggregation methods
        "sum", "mean", "min", "max", "count", "first", "last",
        "std", "var", "median", "n_unique",
        # Comparison
        "eq", "ne", "lt", "le", "gt", "ge",
        # Boolean
        "and_", "or_", "not_",
        # Casting
        "cast",
        # Alias
        "alias",
        # Over (window)
        "over",
        # Conditional (when/then/otherwise)
        "then", "otherwise", "when",
    }
    
    # Binary operators mapping
    BINARY_OPS = {
        ast.Add: operator.add,
        ast.Sub: operator.sub,
        ast.Mult: operator.mul,
        ast.Div: operator.truediv,
        ast.FloorDiv: operator.floordiv,
        ast.Mod: operator.mod,
        ast.Pow: operator.pow,
        ast.Eq: operator.eq,
        ast.NotEq: operator.ne,
        ast.Lt: operator.lt,
        ast.LtE: operator.le,
        ast.Gt: operator.gt,
        ast.GtE: operator.ge,
        ast.BitAnd: operator.and_,
        ast.BitOr: operator.or_,
    }
    
    # Unary operators mapping
    UNARY_OPS = {
        ast.USub: operator.neg,
        ast.UAdd: operator.pos,
        ast.Invert: operator.invert,
        ast.Not: operator.not_,
    }
    
    def __init__(self) -> None:
        """Initialize the expression parser."""
        self._polars_funcs: Dict[str, Any] = {
            "col": pl.col,
            "lit": pl.lit,
            "when": pl.when,
            "sum": pl.sum,
            "mean": pl.mean,
            "min": pl.min,
            "max": pl.max,
            "count": pl.count,
            "first": pl.first,
            "last": pl.last,
            "coalesce": pl.coalesce,
            "concat_str": pl.concat_str,
        }
    
    def parse(self, expression: str) -> pl.Expr:
        """
        Parse a string expression into a Polars expression.
        
        Args:
            expression: String representation of the expression
            
        Returns:
            Polars expression object
            
        Raises:
            ExpressionParseError: If expression is invalid or uses
                disallowed functions/methods
        """
        if not expression or not expression.strip():
            raise ExpressionParseError("Empty expression", expression)
        
        try:
            tree = ast.parse(expression, mode='eval')
            return self._eval_node(tree.body)
        except SyntaxError as e:
            raise ExpressionParseError(
                f"Syntax error: {e.msg}",
                expression,
                e.offset,
            )
        except Exception as e:
            raise ExpressionParseError(str(e), expression)
    
    def validate(self, expression: str) -> Optional[str]:
        """
        Validate expression without executing it.
        
        Args:
            expression: String expression to validate
            
        Returns:
            None if valid, error message if invalid
        """
        try:
            self.parse(expression)
            return None
        except ExpressionParseError as e:
            return str(e)
    
    def _eval_node(self, node: ast.AST) -> Any:
        """Recursively evaluate an AST node."""
        if isinstance(node, ast.Expression):
            return self._eval_node(node.body)
        
        elif isinstance(node, ast.Constant):
            return pl.lit(node.value) if not isinstance(node.value, (int, float, bool, str, type(None))) else node.value
        
        elif isinstance(node, ast.Num):  # Python 3.7 compatibility
            return node.n
        
        elif isinstance(node, ast.Str):  # Python 3.7 compatibility
            return node.s
        
        elif isinstance(node, ast.NameConstant):  # Python 3.7 compatibility
            return node.value
        
        elif isinstance(node, ast.Name):
            name = node.id
            if name == "True":
                return True
            elif name == "False":
                return False
            elif name == "None":
                return None
            elif name in self._polars_funcs:
                return self._polars_funcs[name]
            else:
                raise ExpressionParseError(
                    f"Unknown name: {name}",
                    ast.dump(node),
                )
        
        elif isinstance(node, ast.Call):
            return self._eval_call(node)
        
        elif isinstance(node, ast.Attribute):
            return self._eval_attribute(node)
        
        elif isinstance(node, ast.BinOp):
            return self._eval_binop(node)
        
        elif isinstance(node, ast.UnaryOp):
            return self._eval_unaryop(node)
        
        elif isinstance(node, ast.Compare):
            return self._eval_compare(node)
        
        elif isinstance(node, ast.BoolOp):
            return self._eval_boolop(node)
        
        elif isinstance(node, ast.List):
            return [self._eval_node(elt) for elt in node.elts]
        
        elif isinstance(node, ast.Tuple):
            return tuple(self._eval_node(elt) for elt in node.elts)
        
        elif isinstance(node, ast.Dict):
            return {
                self._eval_node(k): self._eval_node(v)
                for k, v in zip(node.keys, node.values)
            }
        
        else:
            raise ExpressionParseError(
                f"Unsupported expression type: {type(node).__name__}",
                ast.dump(node),
            )
    
    def _eval_call(self, node: ast.Call) -> Any:
        """Evaluate a function call."""
        if isinstance(node.func, ast.Name):
            func_name = node.func.id
            if func_name not in self.ALLOWED_FUNCTIONS:
                raise ExpressionParseError(
                    f"Function '{func_name}' is not allowed",
                    ast.dump(node),
                )
            
            func = self._polars_funcs.get(func_name)
            if func is None:
                raise ExpressionParseError(
                    f"Function '{func_name}' is not implemented",
                    ast.dump(node),
                )
            
            args = [self._eval_node(arg) for arg in node.args]
            kwargs = {kw.arg: self._eval_node(kw.value) for kw in node.keywords}
            
            return func(*args, **kwargs)
        
        elif isinstance(node.func, ast.Attribute):
            # Method call on an expression
            obj = self._eval_node(node.func.value)
            method_name = node.func.attr
            
            if method_name not in self.ALLOWED_METHODS:
                raise ExpressionParseError(
                    f"Method '{method_name}' is not allowed",
                    ast.dump(node),
                )
            
            args = [self._eval_node(arg) for arg in node.args]
            kwargs = {kw.arg: self._eval_node(kw.value) for kw in node.keywords}
            
            method = getattr(obj, method_name, None)
            if method is None:
                raise ExpressionParseError(
                    f"Method '{method_name}' not found on object",
                    ast.dump(node),
                )
            
            return method(*args, **kwargs)
        
        else:
            raise ExpressionParseError(
                f"Unsupported call type: {type(node.func).__name__}",
                ast.dump(node),
            )
    
    def _eval_attribute(self, node: ast.Attribute) -> Any:
        """Evaluate an attribute access."""
        obj = self._eval_node(node.value)
        attr_name = node.attr
        
        if attr_name not in self.ALLOWED_METHODS:
            raise ExpressionParseError(
                f"Attribute '{attr_name}' is not allowed",
                ast.dump(node),
            )
        
        return getattr(obj, attr_name)
    
    def _eval_binop(self, node: ast.BinOp) -> Any:
        """Evaluate a binary operation."""
        left = self._eval_node(node.left)
        right = self._eval_node(node.right)
        
        op_type = type(node.op)
        if op_type not in self.BINARY_OPS:
            raise ExpressionParseError(
                f"Unsupported binary operator: {op_type.__name__}",
                ast.dump(node),
            )
        
        op = self.BINARY_OPS[op_type]
        return op(left, right)
    
    def _eval_unaryop(self, node: ast.UnaryOp) -> Any:
        """Evaluate a unary operation."""
        operand = self._eval_node(node.operand)
        
        op_type = type(node.op)
        if op_type not in self.UNARY_OPS:
            raise ExpressionParseError(
                f"Unsupported unary operator: {op_type.__name__}",
                ast.dump(node),
            )
        
        op = self.UNARY_OPS[op_type]
        return op(operand)
    
    def _eval_compare(self, node: ast.Compare) -> Any:
        """Evaluate a comparison expression."""
        left = self._eval_node(node.left)
        
        result = None
        current = left
        
        for op, comparator in zip(node.ops, node.comparators):
            right = self._eval_node(comparator)
            
            op_type = type(op)
            if op_type not in self.BINARY_OPS:
                raise ExpressionParseError(
                    f"Unsupported comparison operator: {op_type.__name__}",
                    ast.dump(node),
                )
            
            comparison = self.BINARY_OPS[op_type](current, right)
            
            if result is None:
                result = comparison
            else:
                result = result & comparison
            
            current = right
        
        return result
    
    def _eval_boolop(self, node: ast.BoolOp) -> Any:
        """Evaluate a boolean operation (and, or)."""
        values = [self._eval_node(v) for v in node.values]
        
        if isinstance(node.op, ast.And):
            result = values[0]
            for v in values[1:]:
                result = result & v
            return result
        elif isinstance(node.op, ast.Or):
            result = values[0]
            for v in values[1:]:
                result = result | v
            return result
        else:
            raise ExpressionParseError(
                f"Unsupported boolean operator: {type(node.op).__name__}",
                ast.dump(node),
            )
