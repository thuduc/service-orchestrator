"""Test fixtures for Data Transformation Framework tests."""

import pytest
import polars as pl

from frameworks.data_transformation.engine.transformation_engine import TransformationEngine
from frameworks.data_transformation.engine.transformation_context import TransformationContext


@pytest.fixture
def sample_customers_df() -> pl.DataFrame:
    """Sample customer DataFrame for testing."""
    return pl.DataFrame({
        "customer_id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "email": ["alice@test.com", "bob@test.com", "charlie@test.com", 
                  "diana@test.com", "eve@test.com"],
        "status": ["active", "active", "inactive", "active", "inactive"],
        "age": [25, 30, 35, 28, 42],
        "signup_date": ["2023-01-15", "2023-02-20", "2023-03-10", 
                        "2023-04-05", "2023-05-12"],
    })


@pytest.fixture
def sample_orders_df() -> pl.DataFrame:
    """Sample orders DataFrame for testing joins."""
    return pl.DataFrame({
        "order_id": [101, 102, 103, 104, 105, 106],
        "customer_id": [1, 1, 2, 3, 1, 4],
        "amount": [100.0, 150.0, 200.0, 75.0, 300.0, 125.0],
        "order_date": ["2023-06-01", "2023-06-15", "2023-06-10",
                       "2023-06-20", "2023-07-01", "2023-07-05"],
        "status": ["completed", "completed", "completed", 
                   "cancelled", "pending", "completed"],
    })


@pytest.fixture
def sample_products_df() -> pl.DataFrame:
    """Sample products DataFrame for testing."""
    return pl.DataFrame({
        "product_id": [1, 2, 3, 4],
        "name": ["Widget", "Gadget", "Gizmo", "Doohickey"],
        "price": [10.99, 24.99, 5.49, 15.00],
        "category": ["electronics", "electronics", "accessories", "tools"],
    })


@pytest.fixture
def df_with_nulls() -> pl.DataFrame:
    """DataFrame with null values for testing null handling."""
    return pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", None, "Charlie", "Diana", None],
        "value": [100.0, 200.0, None, 400.0, 500.0],
        "category": ["A", "B", None, "A", "B"],
    })


@pytest.fixture
def df_with_lists() -> pl.DataFrame:
    """DataFrame with list columns for testing explode."""
    return pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "tags": [["python", "data"], ["java", "web"], ["python", "ml", "ai"]],
    })


@pytest.fixture
def empty_context(sample_customers_df) -> TransformationContext:
    """Empty transformation context for testing."""
    return TransformationContext(data=sample_customers_df)


@pytest.fixture
def context_with_datasets(
    sample_customers_df,
    sample_orders_df,
    sample_products_df,
) -> TransformationContext:
    """Context with additional datasets for testing joins."""
    return TransformationContext(
        data=sample_customers_df,
        datasets={
            "orders": sample_orders_df,
            "products": sample_products_df,
        },
    )


@pytest.fixture
def transformation_engine() -> TransformationEngine:
    """Fresh transformation engine instance."""
    return TransformationEngine()
