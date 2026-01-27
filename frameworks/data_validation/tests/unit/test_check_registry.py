"""Unit tests for CheckRegistry."""

import pytest

from frameworks.data_validation.contract.check import CheckResult, CustomCheck
from frameworks.data_validation.registries.check_registry import CheckRegistry


class MockCheck(CustomCheck):
    """Mock check for testing."""

    def __init__(self, threshold: int = 10) -> None:
        self.threshold = threshold

    def validate_column(self, df, column: str) -> CheckResult:
        return CheckResult(is_valid=True, message="Mock check passed")


class AnotherMockCheck(CustomCheck):
    """Another mock check for testing."""

    def validate_column(self, df, column: str) -> CheckResult:
        return CheckResult(is_valid=False, message="Mock check failed")


class TestCheckRegistry:
    """Test cases for CheckRegistry."""

    def test_register_check(self, check_registry: CheckRegistry) -> None:
        """Test registering a check."""
        check_registry.register("mock_check", MockCheck)
        
        assert check_registry.has("mock_check")
        assert check_registry.get("mock_check") is MockCheck

    def test_register_with_default_params(self, check_registry: CheckRegistry) -> None:
        """Test registering a check with default parameters."""
        check_registry.register("mock_check", MockCheck, {"threshold": 20})
        
        assert check_registry.get_default_params("mock_check") == {"threshold": 20}

    def test_register_duplicate_raises_error(self, check_registry: CheckRegistry) -> None:
        """Test that registering a duplicate check raises ValueError."""
        check_registry.register("mock_check", MockCheck)
        
        with pytest.raises(ValueError, match="already registered"):
            check_registry.register("mock_check", AnotherMockCheck)

    def test_register_duplicate_with_overwrite(self, check_registry: CheckRegistry) -> None:
        """Test that overwrite=True allows replacing a check."""
        check_registry.register("mock_check", MockCheck)
        check_registry.register("mock_check", AnotherMockCheck, overwrite=True)
        
        assert check_registry.get("mock_check") is AnotherMockCheck

    def test_register_invalid_class_raises_error(self, check_registry: CheckRegistry) -> None:
        """Test that registering a non-CustomCheck raises TypeError."""
        with pytest.raises(TypeError, match="must be a subclass of CustomCheck"):
            check_registry.register("invalid", str)  # type: ignore

    def test_unregister_check(self, check_registry: CheckRegistry) -> None:
        """Test unregistering a check."""
        check_registry.register("mock_check", MockCheck, {"threshold": 20})
        check_registry.unregister("mock_check")
        
        assert not check_registry.has("mock_check")
        assert check_registry.get_default_params("mock_check") == {}

    def test_unregister_nonexistent_raises_error(self, check_registry: CheckRegistry) -> None:
        """Test that unregistering a non-existent check raises KeyError."""
        with pytest.raises(KeyError, match="not registered"):
            check_registry.unregister("nonexistent")

    def test_get_nonexistent_returns_none(self, check_registry: CheckRegistry) -> None:
        """Test that getting a non-existent check returns None."""
        assert check_registry.get("nonexistent") is None

    def test_get_merged_params(self, check_registry: CheckRegistry) -> None:
        """Test merging default params with rule params."""
        check_registry.register("mock_check", MockCheck, {"threshold": 20, "option": "default"})
        
        merged = check_registry.get_merged_params(
            "mock_check", 
            {"threshold": 30, "extra": "value"}
        )
        
        assert merged == {"threshold": 30, "option": "default", "extra": "value"}

    def test_list_checks(self, check_registry: CheckRegistry) -> None:
        """Test listing all registered checks."""
        check_registry.register("check_a", MockCheck)
        check_registry.register("check_b", AnotherMockCheck)
        
        checks = check_registry.list_checks()
        
        assert set(checks) == {"check_a", "check_b"}

    def test_clear(self, check_registry: CheckRegistry) -> None:
        """Test clearing all checks."""
        check_registry.register("check_a", MockCheck)
        check_registry.register("check_b", AnotherMockCheck)
        check_registry.clear()
        
        assert len(check_registry) == 0
        assert check_registry.list_checks() == []

    def test_len(self, check_registry: CheckRegistry) -> None:
        """Test __len__ method."""
        assert len(check_registry) == 0
        
        check_registry.register("check_a", MockCheck)
        assert len(check_registry) == 1
        
        check_registry.register("check_b", AnotherMockCheck)
        assert len(check_registry) == 2

    def test_contains(self, check_registry: CheckRegistry) -> None:
        """Test __contains__ method."""
        check_registry.register("mock_check", MockCheck)
        
        assert "mock_check" in check_registry
        assert "nonexistent" not in check_registry
