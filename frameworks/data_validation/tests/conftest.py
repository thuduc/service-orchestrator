"""Shared pytest fixtures for Data Validation Framework tests."""

from datetime import date
from typing import Any, Dict

import polars as pl
import pytest

from frameworks.data_validation.engine.validation_engine import ValidationEngine
from frameworks.data_validation.registries.check_registry import CheckRegistry
from frameworks.data_validation.registries.stage_registry import StageRegistry


@pytest.fixture
def check_registry() -> CheckRegistry:
    """Create a fresh check registry for testing."""
    return CheckRegistry()


@pytest.fixture
def stage_registry() -> StageRegistry:
    """Create a fresh stage registry for testing."""
    return StageRegistry()


@pytest.fixture
def sample_customer_df() -> pl.DataFrame:
    """Create a sample customer DataFrame for testing."""
    return pl.DataFrame({
        "customer_id": [1, 2, 3, 4, 5],
        "email": [
            "john@gmail.com",
            "jane@yahoo.com",
            "bob@company.com",
            "invalid-email",
            "alice@outlook.com",
        ],
        "first_name": ["John", "Jane", "Bob", "Charlie", "Alice"],
        "last_name": ["Doe", "Smith", "Johnson", "Brown", "Williams"],
        "age": [25, 35, None, 17, 45],
        "registration_date": [
            date(2023, 1, 15),
            date(2023, 2, 20),
            date(2023, 3, 10),
            date(2023, 4, 5),
            date(2025, 12, 31),  # Future date
        ],
        "status": ["active", "active", "inactive", "pending", "suspended"],
        "country_code": ["US", "CA", None, "GB", "AU"],
    })


@pytest.fixture
def sample_order_df() -> pl.DataFrame:
    """Create a sample order DataFrame for testing."""
    return pl.DataFrame({
        "order_id": [1001, 1002, 1003, 1004],
        "customer_id": [1, 2, 3, 999],  # 999 doesn't exist
        "order_date": [
            date(2023, 5, 1),
            date(2023, 5, 10),
            date(2023, 5, 15),
            date(2023, 5, 20),
        ],
        "ship_date": [
            date(2023, 5, 5),
            None,
            date(2023, 5, 14),  # Before order date - invalid
            date(2023, 5, 25),
        ],
        "subtotal": [100.00, 50.00, 75.00, 200.00],
        "tax": [10.00, 5.00, 7.50, 20.00],
        "total": [110.00, 55.00, 80.00, 220.00],  # 80.00 is wrong (should be 82.50)
        "status": ["shipped", "pending", "delivered", "confirmed"],
    })


@pytest.fixture
def sample_product_df() -> pl.DataFrame:
    """Create a sample product DataFrame for testing."""
    return pl.DataFrame({
        "product_id": [1, 2, 3, 4, 5],
        "name": ["Laptop", "Phone", "Tablet", "", "Watch"],  # Empty name
        "price": [999.99, 699.99, -199.99, 299.99, 0.00],  # Negative and zero prices
        "category": ["electronics", "electronics", "electronics", "invalid", "clothing"],
        "stock_quantity": [10, 50, -5, 100, 25],  # Negative stock
    })


@pytest.fixture
def countries_reference_df() -> pl.DataFrame:
    """Create a countries reference DataFrame for testing."""
    return pl.DataFrame({
        "code": ["US", "CA", "GB", "DE", "FR", "AU", "JP"],
        "name": [
            "United States", "Canada", "United Kingdom",
            "Germany", "France", "Australia", "Japan"
        ],
    })


@pytest.fixture
def customers_reference_df() -> pl.DataFrame:
    """Create a customers reference DataFrame for testing."""
    return pl.DataFrame({
        "customer_id": [1, 2, 3, 4, 5],
        "name": ["John Doe", "Jane Smith", "Bob Johnson", "Charlie Brown", "Alice Williams"],
    })


@pytest.fixture
def simple_pipeline_config() -> Dict[str, Any]:
    """Create a simple pipeline configuration for testing."""
    return {
        "description": "Simple test pipeline",
        "on_failure": "collect_all",
        "stages": [
            {
                "name": "validate_schema",
                "type": "schema_validation",
                "config": {
                    "columns": {
                        "id": {
                            "dtype": "Int64",
                            "nullable": False,
                        },
                        "name": {
                            "dtype": "Utf8",
                            "nullable": False,
                        },
                        "value": {
                            "dtype": "Float64",
                            "nullable": True,
                        },
                    },
                },
            },
        ],
    }


@pytest.fixture
def simple_df() -> pl.DataFrame:
    """Create a simple DataFrame for testing."""
    return pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [10.5, None, 30.0],
    })


@pytest.fixture
def invalid_simple_df() -> pl.DataFrame:
    """Create a simple DataFrame with invalid data for testing."""
    return pl.DataFrame({
        "id": [1, None, 3],  # Null ID
        "name": ["Alice", "Bob", ""],  # Empty name
        "value": ["not_a_number", None, 30.0],  # Invalid type
    })


@pytest.fixture
def validation_engine(tmp_path) -> ValidationEngine:
    """Create a ValidationEngine with example configs for testing."""
    # Create a minimal engine without loading config files
    engine = ValidationEngine()
    return engine
