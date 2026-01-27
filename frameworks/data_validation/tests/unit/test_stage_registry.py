"""Unit tests for StageRegistry."""

import pytest

from frameworks.data_validation.contract.validation_stage import ValidationStage
from frameworks.data_validation.engine.validation_context import ValidationContext
from frameworks.data_validation.engine.validation_result import StageResult
from frameworks.data_validation.registries.stage_registry import StageRegistry


class MockStage(ValidationStage):
    """Mock stage for testing."""

    def __init__(self, name: str, config: dict) -> None:
        self._name = name

    @property
    def stage_type(self) -> str:
        return "mock_stage"

    @property
    def name(self) -> str:
        return self._name

    def execute(self, context: ValidationContext) -> StageResult:
        return StageResult(
            stage_name=self.name,
            stage_type=self.stage_type,
            is_valid=True,
            errors=[],
            warnings=[],
            execution_time_ms=0.0,
            rows_validated=0,
            rows_failed=0,
        )


class AnotherMockStage(ValidationStage):
    """Another mock stage for testing."""

    def __init__(self, name: str, config: dict) -> None:
        self._name = name

    @property
    def stage_type(self) -> str:
        return "another_mock_stage"

    @property
    def name(self) -> str:
        return self._name

    def execute(self, context: ValidationContext) -> StageResult:
        return StageResult(
            stage_name=self.name,
            stage_type=self.stage_type,
            is_valid=False,
            errors=[],
            warnings=[],
            execution_time_ms=0.0,
            rows_validated=0,
            rows_failed=0,
        )


class TestStageRegistry:
    """Test cases for StageRegistry."""

    def test_register_stage(self, stage_registry: StageRegistry) -> None:
        """Test registering a stage."""
        stage_registry.register("mock_stage", MockStage)
        
        assert stage_registry.has("mock_stage")
        assert stage_registry.get("mock_stage") is MockStage

    def test_register_duplicate_raises_error(self, stage_registry: StageRegistry) -> None:
        """Test that registering a duplicate stage raises ValueError."""
        stage_registry.register("mock_stage", MockStage)
        
        with pytest.raises(ValueError, match="already registered"):
            stage_registry.register("mock_stage", AnotherMockStage)

    def test_register_duplicate_with_overwrite(self, stage_registry: StageRegistry) -> None:
        """Test that overwrite=True allows replacing a stage."""
        stage_registry.register("mock_stage", MockStage)
        stage_registry.register("mock_stage", AnotherMockStage, overwrite=True)
        
        assert stage_registry.get("mock_stage") is AnotherMockStage

    def test_unregister_stage(self, stage_registry: StageRegistry) -> None:
        """Test unregistering a stage."""
        stage_registry.register("mock_stage", MockStage)
        stage_registry.unregister("mock_stage")
        
        assert not stage_registry.has("mock_stage")

    def test_unregister_nonexistent_raises_error(self, stage_registry: StageRegistry) -> None:
        """Test that unregistering a non-existent stage raises KeyError."""
        with pytest.raises(KeyError, match="not registered"):
            stage_registry.unregister("nonexistent")

    def test_get_nonexistent_returns_none(self, stage_registry: StageRegistry) -> None:
        """Test that getting a non-existent stage returns None."""
        assert stage_registry.get("nonexistent") is None

    def test_list_stages(self, stage_registry: StageRegistry) -> None:
        """Test listing all registered stages."""
        stage_registry.register("stage_a", MockStage)
        stage_registry.register("stage_b", AnotherMockStage)
        
        stages = stage_registry.list_stages()
        
        assert set(stages) == {"stage_a", "stage_b"}

    def test_clear(self, stage_registry: StageRegistry) -> None:
        """Test clearing all stages."""
        stage_registry.register("stage_a", MockStage)
        stage_registry.register("stage_b", AnotherMockStage)
        stage_registry.clear()
        
        assert len(stage_registry) == 0
        assert stage_registry.list_stages() == []

    def test_len(self, stage_registry: StageRegistry) -> None:
        """Test __len__ method."""
        assert len(stage_registry) == 0
        
        stage_registry.register("stage_a", MockStage)
        assert len(stage_registry) == 1

    def test_contains(self, stage_registry: StageRegistry) -> None:
        """Test __contains__ method."""
        stage_registry.register("mock_stage", MockStage)
        
        assert "mock_stage" in stage_registry
        assert "nonexistent" not in stage_registry
