import pytest
from typing import Dict, Any
from unittest.mock import Mock
from frameworks.service_pipeline.orchestration.interceptor_pipeline import (
    InterceptorPipeline,
    InterceptorShortCircuit
)
from frameworks.service_pipeline.contract import Interceptor


class MockInterceptor(Interceptor):
    """Mock interceptor for testing"""

    def __init__(self, name="mock"):
        self.name = name
        self.before_called = False
        self.after_called = False
        self.on_error_called = False
        self.before_context = None
        self.after_context = None
        self.after_result = None
        self.error_context = None
        self.error = None

    def before(self, context):
        self.before_called = True
        self.before_context = context.copy()
        return context

    def after(self, context, result):
        self.after_called = True
        self.after_context = context.copy()
        self.after_result = result.copy() if result else None
        return result

    def on_error(self, context, error):
        self.on_error_called = True
        self.error_context = context.copy()
        self.error = error
        return None  # Re-raise


class TestInterceptorPipeline:

    def test_init(self):
        """Test pipeline initialization."""
        pipeline = InterceptorPipeline()
        assert pipeline.interceptors == []

    def test_add_interceptor(self):
        """Test adding interceptor to pipeline."""
        pipeline = InterceptorPipeline()
        interceptor1 = MockInterceptor("first")
        interceptor2 = MockInterceptor("second")

        pipeline.add_interceptor(interceptor1)
        pipeline.add_interceptor(interceptor2)

        assert len(pipeline.interceptors) == 2
        assert pipeline.interceptors[0] == interceptor1
        assert pipeline.interceptors[1] == interceptor2

    def test_clear_interceptors(self):
        """Test clearing all interceptors."""
        pipeline = InterceptorPipeline()
        interceptor = MockInterceptor()
        pipeline.add_interceptor(interceptor)

        assert len(pipeline.interceptors) == 1

        pipeline.clear_interceptors()
        assert pipeline.interceptors == []

    def test_execute_no_interceptors(self):
        """Test execution with no interceptors."""
        pipeline = InterceptorPipeline()
        component = Mock()
        context = {"test": "context"}
        expected_result = {"result": "success"}

        component.execute.return_value = expected_result

        result = pipeline.execute(context, component)

        assert result == expected_result
        component.execute.assert_called_once_with(context)

    def test_execute_single_interceptor(self):
        """Test execution with single interceptor."""
        pipeline = InterceptorPipeline()
        interceptor = MockInterceptor()
        component = Mock()
        context = {"test": "context"}
        component_result = {"component": "result"}

        component.execute.return_value = component_result

        pipeline.add_interceptor(interceptor)
        result = pipeline.execute(context, component)

        assert result == component_result
        assert interceptor.before_called
        assert interceptor.after_called
        assert not interceptor.on_error_called
        component.execute.assert_called_once_with(context)

    def test_execute_multiple_interceptors(self):
        """Test execution with multiple interceptors."""
        pipeline = InterceptorPipeline()
        interceptor1 = MockInterceptor("first")
        interceptor2 = MockInterceptor("second")
        component = Mock()
        context = {"test": "context"}

        component.execute.return_value = {"component": "result"}

        pipeline.add_interceptor(interceptor1)
        pipeline.add_interceptor(interceptor2)

        result = pipeline.execute(context, component)

        # Both interceptors should have before and after called
        assert interceptor1.before_called
        assert interceptor1.after_called
        assert interceptor2.before_called
        assert interceptor2.after_called
        component.execute.assert_called_once_with(context)

    def test_execute_interceptor_order(self):
        """Test that interceptors execute in correct order."""
        pipeline = InterceptorPipeline()
        execution_order = []

        class OrderTrackingInterceptor(Interceptor):
            def __init__(self, name):
                self.name = name

            def before(self, context):
                execution_order.append(f"{self.name}_before")
                return context

            def after(self, context, result):
                execution_order.append(f"{self.name}_after")
                return result

        interceptor1 = OrderTrackingInterceptor("first")
        interceptor2 = OrderTrackingInterceptor("second")
        component = Mock()
        context = {"test": "context"}

        def component_execute(ctx):
            execution_order.append("component")
            return {"component": "result"}

        component.execute.side_effect = component_execute

        pipeline.add_interceptor(interceptor1)
        pipeline.add_interceptor(interceptor2)

        pipeline.execute(context, component)

        # before() in order, then component, then after() in reverse
        expected_order = [
            "first_before",
            "second_before",
            "component",
            "second_after",
            "first_after"
        ]
        assert execution_order == expected_order

    def test_execute_interceptor_modifies_context(self):
        """Test that interceptor can modify context."""
        pipeline = InterceptorPipeline()

        class ModifyingInterceptor(Interceptor):
            def before(self, context):
                context["interceptor_added"] = "value"
                return context

        interceptor = ModifyingInterceptor()
        component = Mock()
        original_context = {"original": "data"}

        component.execute.return_value = {"result": "success"}

        pipeline.add_interceptor(interceptor)
        result = pipeline.execute(original_context, component)

        # Verify component received modified context
        component.execute.assert_called_once()
        call_args = component.execute.call_args[0][0]
        assert call_args["original"] == "data"
        assert call_args["interceptor_added"] == "value"

    def test_execute_interceptor_modifies_result(self):
        """Test that interceptor can modify result."""
        pipeline = InterceptorPipeline()

        class ModifyingInterceptor(Interceptor):
            def after(self, context, result):
                result["interceptor_added"] = "value"
                return result

        interceptor = ModifyingInterceptor()
        component = Mock()
        context = {"test": "context"}

        component.execute.return_value = {"component": "result"}

        pipeline.add_interceptor(interceptor)
        result = pipeline.execute(context, component)

        assert result["component"] == "result"
        assert result["interceptor_added"] == "value"

    def test_execute_component_exception_calls_on_error(self):
        """Test that exceptions from component trigger on_error."""
        pipeline = InterceptorPipeline()
        interceptor = MockInterceptor()
        component = Mock()
        context = {"test": "context"}

        component.execute.side_effect = Exception("Component error")

        pipeline.add_interceptor(interceptor)

        with pytest.raises(Exception, match="Component error"):
            pipeline.execute(context, component)

        assert interceptor.before_called
        assert not interceptor.after_called
        assert interceptor.on_error_called
        assert str(interceptor.error) == "Component error"

    def test_execute_on_error_can_recover(self):
        """Test that on_error can recover from exceptions."""
        pipeline = InterceptorPipeline()

        class RecoveringInterceptor(Interceptor):
            def on_error(self, context, error):
                return {"recovered": True, "original_error": str(error)}

        interceptor = RecoveringInterceptor()
        component = Mock()
        context = {"test": "context"}

        component.execute.side_effect = Exception("Component error")

        pipeline.add_interceptor(interceptor)
        result = pipeline.execute(context, component)

        assert result["recovered"] is True
        assert result["original_error"] == "Component error"

    def test_execute_before_short_circuit(self):
        """Test interceptor short-circuiting by returning None from before()."""
        pipeline = InterceptorPipeline()

        class ShortCircuitInterceptor(Interceptor):
            def before(self, context) -> Dict[str, Any]:
                return None  # type: ignore[return-value]

        interceptor = ShortCircuitInterceptor()
        component = Mock()
        context = {"test": "context"}

        pipeline.add_interceptor(interceptor)

        with pytest.raises(InterceptorShortCircuit):
            pipeline.execute(context, component)

        component.execute.assert_not_called()

    def test_execute_complex_interceptor_chain(self):
        """Test complex interceptor chain with different behaviors."""
        pipeline = InterceptorPipeline()

        class AuthInterceptor(Interceptor):
            def before(self, context):
                if context.get("user") != "test":
                    raise ValueError("Unauthorized")
                context["authenticated"] = True
                return context

        class LoggingInterceptor(Interceptor):
            def __init__(self):
                self.logs = []

            def before(self, context):
                self.logs.append("request_start")
                return context

            def after(self, context, result):
                self.logs.append("request_end")
                result["logged"] = True
                return result

        auth_interceptor = AuthInterceptor()
        logging_interceptor = LoggingInterceptor()
        component = Mock()
        context = {"user": "test", "data": "input"}

        component.execute.return_value = {"processed": True}

        pipeline.add_interceptor(auth_interceptor)
        pipeline.add_interceptor(logging_interceptor)

        result = pipeline.execute(context, component)

        assert result["processed"] is True
        assert result["logged"] is True
        assert logging_interceptor.logs == ["request_start", "request_end"]

        # Verify component received the authenticated context
        component.execute.assert_called_once()
        call_args = component.execute.call_args[0][0]
        assert call_args["authenticated"] is True

    def test_multiple_interceptors_on_error_chain(self):
        """Test that on_error is called for all interceptors that had before() called."""
        pipeline = InterceptorPipeline()
        error_calls = []

        class TrackingInterceptor(Interceptor):
            def __init__(self, name):
                self.name = name

            def before(self, context):
                return context

            def on_error(self, context, error):
                error_calls.append(self.name)
                return None  # Re-raise

        interceptor1 = TrackingInterceptor("first")
        interceptor2 = TrackingInterceptor("second")
        component = Mock()
        context = {"test": "context"}

        component.execute.side_effect = Exception("Component error")

        pipeline.add_interceptor(interceptor1)
        pipeline.add_interceptor(interceptor2)

        with pytest.raises(Exception, match="Component error"):
            pipeline.execute(context, component)

        # on_error should be called in reverse order
        assert error_calls == ["second", "first"]
