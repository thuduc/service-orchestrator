import pytest
from unittest.mock import Mock, patch, MagicMock
from framework.steps_executor import StepsExecutor


class TestStepsExecutor:

    @patch('framework.steps_executor.importlib.import_module')
    def test_init_single_step(self, mock_import):
        """Test initializing executor with single step."""
        mock_module = Mock()
        mock_component_class = Mock()
        mock_component_instance = Mock()

        mock_import.return_value = mock_module
        mock_module.TestComponent = mock_component_class
        mock_component_class.return_value = mock_component_instance

        steps_config = [
            {
                "name": "test_step",
                "module": "test.module",
                "class": "TestComponent",
                "config": {"param": "value"}
            }
        ]

        executor = StepsExecutor(steps_config)

        assert len(executor.steps) == 1
        step = executor.steps[0]
        assert step["name"] == "test_step"
        assert step["component"] == mock_component_instance
        assert step["on_error"] == "fail_fast"
        assert step["fallback_output"] == {}

        mock_import.assert_called_once_with("test.module")
        mock_component_class.assert_called_once_with({"param": "value"})

    @patch('framework.steps_executor.importlib.import_module')
    def test_init_multiple_steps(self, mock_import):
        """Test initializing executor with multiple steps."""
        mock_module1 = Mock()
        mock_module2 = Mock()
        mock_component_class1 = Mock()
        mock_component_class2 = Mock()
        mock_instance1 = Mock()
        mock_instance2 = Mock()

        def import_side_effect(module_name):
            if module_name == "module1":
                return mock_module1
            elif module_name == "module2":
                return mock_module2

        mock_import.side_effect = import_side_effect
        mock_module1.Component1 = mock_component_class1
        mock_module2.Component2 = mock_component_class2
        mock_component_class1.return_value = mock_instance1
        mock_component_class2.return_value = mock_instance2

        steps_config = [
            {"name": "step1", "module": "module1", "class": "Component1"},
            {"name": "step2", "module": "module2", "class": "Component2"}
        ]

        executor = StepsExecutor(steps_config)

        assert len(executor.steps) == 2
        assert executor.steps[0]["component"] == mock_instance1
        assert executor.steps[1]["component"] == mock_instance2

    @patch('framework.steps_executor.importlib.import_module')
    def test_init_step_without_name(self, mock_import):
        """Test initializing step without explicit name."""
        mock_module = Mock()
        mock_component_class = Mock()
        mock_instance = Mock()

        mock_import.return_value = mock_module
        mock_module.TestComponent = mock_component_class
        mock_component_class.return_value = mock_instance

        steps_config = [
            {"module": "test.module", "class": "TestComponent"}
        ]

        executor = StepsExecutor(steps_config)

        assert executor.steps[0]["name"] == "unnamed"

    @patch('framework.steps_executor.importlib.import_module')
    def test_init_step_with_error_handling_config(self, mock_import):
        """Test initializing step with error handling configuration."""
        mock_module = Mock()
        mock_component_class = Mock()
        mock_instance = Mock()

        mock_import.return_value = mock_module
        mock_module.TestComponent = mock_component_class
        mock_component_class.return_value = mock_instance

        steps_config = [
            {
                "name": "test_step",
                "module": "test.module",
                "class": "TestComponent",
                "on_error": "skip",
                "fallback_output": {"error": "handled"}
            }
        ]

        executor = StepsExecutor(steps_config)

        step = executor.steps[0]
        assert step["on_error"] == "skip"
        assert step["fallback_output"] == {"error": "handled"}

    @patch('framework.steps_executor.importlib.import_module')
    def test_init_import_error(self, mock_import):
        """Test initialization with import error."""
        mock_import.side_effect = ImportError("Module not found")

        steps_config = [
            {"name": "test_step", "module": "nonexistent.module", "class": "TestComponent"}
        ]

        with pytest.raises(RuntimeError, match="Failed to load step 'test_step'"):
            StepsExecutor(steps_config)

    @patch('framework.steps_executor.importlib.import_module')
    def test_init_attribute_error(self, mock_import):
        """Test initialization with attribute error (class not found)."""
        mock_module = Mock()
        mock_import.return_value = mock_module
        del mock_module.NonexistentClass

        steps_config = [
            {"name": "test_step", "module": "test.module", "class": "NonexistentClass"}
        ]

        with pytest.raises(RuntimeError, match="Failed to load step 'test_step'"):
            StepsExecutor(steps_config)

    @patch('framework.steps_executor.importlib.import_module')
    def test_execute_single_step_success(self, mock_import):
        """Test successful execution of single step."""
        mock_module = Mock()
        mock_component_class = Mock()
        mock_component = Mock()

        mock_import.return_value = mock_module
        mock_module.TestComponent = mock_component_class
        mock_component_class.return_value = mock_component
        mock_component.execute.return_value = {"result": "success"}

        steps_config = [
            {"name": "test_step", "module": "test.module", "class": "TestComponent"}
        ]

        executor = StepsExecutor(steps_config)
        context = {"input": "data"}

        result = executor.execute(context)

        assert result["input"] == "data"
        assert result["result"] == "success"
        mock_component.execute.assert_called_once_with(context)

    @patch('framework.steps_executor.importlib.import_module')
    def test_execute_multiple_steps_success(self, mock_import):
        """Test successful execution of multiple steps."""
        mock_module1 = Mock()
        mock_module2 = Mock()
        mock_component1 = Mock()
        mock_component2 = Mock()

        def import_side_effect(module_name):
            if module_name == "module1":
                return mock_module1
            elif module_name == "module2":
                return mock_module2

        mock_import.side_effect = import_side_effect
        mock_module1.Component1 = Mock(return_value=mock_component1)
        mock_module2.Component2 = Mock(return_value=mock_component2)

        mock_component1.execute.return_value = {"step1": "result1"}
        mock_component2.execute.return_value = {"step2": "result2"}

        steps_config = [
            {"name": "step1", "module": "module1", "class": "Component1"},
            {"name": "step2", "module": "module2", "class": "Component2"}
        ]

        executor = StepsExecutor(steps_config)
        context = {"input": "data"}

        result = executor.execute(context)

        assert result["input"] == "data"
        assert result["step1"] == "result1"
        assert result["step2"] == "result2"
        assert mock_component1.execute.call_count == 1
        assert mock_component2.execute.call_count == 1

    @patch('framework.steps_executor.importlib.import_module')
    def test_execute_step_returns_non_dict(self, mock_import):
        """Test execution when step returns non-dictionary."""
        mock_module = Mock()
        mock_component_class = Mock()
        mock_component = Mock()

        mock_import.return_value = mock_module
        mock_module.TestComponent = mock_component_class
        mock_component_class.return_value = mock_component
        mock_component.execute.return_value = "string_result"

        steps_config = [
            {"name": "test_step", "module": "test.module", "class": "TestComponent"}
        ]

        executor = StepsExecutor(steps_config)
        context = {"input": "data"}

        result = executor.execute(context)

        # Context should remain unchanged when non-dict is returned
        assert result == {"input": "data"}

    @patch('framework.steps_executor.importlib.import_module')
    def test_execute_fail_fast_error(self, mock_import):
        """Test execution with fail_fast error handling."""
        mock_module = Mock()
        mock_component_class = Mock()
        mock_component = Mock()

        mock_import.return_value = mock_module
        mock_module.TestComponent = mock_component_class
        mock_component_class.return_value = mock_component
        mock_component.execute.side_effect = Exception("Component failed")

        steps_config = [
            {
                "name": "failing_step",
                "module": "test.module",
                "class": "TestComponent",
                "on_error": "fail_fast"
            }
        ]

        executor = StepsExecutor(steps_config)
        context = {"input": "data"}

        with pytest.raises(Exception, match="Component failed"):
            executor.execute(context)

    @patch('framework.steps_executor.importlib.import_module')
    def test_execute_skip_error(self, mock_import):
        """Test execution with skip error handling."""
        mock_module1 = Mock()
        mock_module2 = Mock()
        mock_component1 = Mock()
        mock_component2 = Mock()

        def import_side_effect(module_name):
            if module_name == "module1":
                return mock_module1
            elif module_name == "module2":
                return mock_module2

        mock_import.side_effect = import_side_effect
        mock_module1.Component1 = Mock(return_value=mock_component1)
        mock_module2.Component2 = Mock(return_value=mock_component2)

        mock_component1.execute.side_effect = Exception("Step 1 failed")
        mock_component2.execute.return_value = {"step2": "success"}

        steps_config = [
            {
                "name": "failing_step",
                "module": "module1",
                "class": "Component1",
                "on_error": "skip",
                "fallback_output": {"step1": "fallback"}
            },
            {
                "name": "success_step",
                "module": "module2",
                "class": "Component2"
            }
        ]

        executor = StepsExecutor(steps_config)
        context = {"input": "data"}

        result = executor.execute(context)

        # Should have fallback output from failed step and result from successful step
        assert result["input"] == "data"
        assert result["step1"] == "fallback"
        assert result["step2"] == "success"

    @patch('framework.steps_executor.importlib.import_module')
    def test_execute_skip_error_no_fallback(self, mock_import):
        """Test execution with skip error handling but no fallback output."""
        mock_module1 = Mock()
        mock_module2 = Mock()
        mock_component1 = Mock()
        mock_component2 = Mock()

        def import_side_effect(module_name):
            if module_name == "module1":
                return mock_module1
            elif module_name == "module2":
                return mock_module2

        mock_import.side_effect = import_side_effect
        mock_module1.Component1 = Mock(return_value=mock_component1)
        mock_module2.Component2 = Mock(return_value=mock_component2)

        mock_component1.execute.side_effect = Exception("Step 1 failed")
        mock_component2.execute.return_value = {"step2": "success"}

        steps_config = [
            {
                "name": "failing_step",
                "module": "module1",
                "class": "Component1",
                "on_error": "skip"
            },
            {
                "name": "success_step",
                "module": "module2",
                "class": "Component2"
            }
        ]

        executor = StepsExecutor(steps_config)
        context = {"input": "data"}

        result = executor.execute(context)

        # Should only have input and successful step result
        assert result["input"] == "data"
        assert result["step2"] == "success"
        assert "step1" not in result

    @patch('framework.steps_executor.importlib.import_module')
    def test_execute_compensate_error_not_implemented(self, mock_import):
        """Test execution with compensate error handling (not implemented)."""
        mock_module = Mock()
        mock_component_class = Mock()
        mock_component = Mock()

        mock_import.return_value = mock_module
        mock_module.TestComponent = mock_component_class
        mock_component_class.return_value = mock_component
        mock_component.execute.side_effect = Exception("Component failed")

        steps_config = [
            {
                "name": "failing_step",
                "module": "test.module",
                "class": "TestComponent",
                "on_error": "compensate"
            }
        ]

        executor = StepsExecutor(steps_config)
        context = {"input": "data"}

        with pytest.raises(NotImplementedError, match="Compensation not yet implemented"):
            executor.execute(context)