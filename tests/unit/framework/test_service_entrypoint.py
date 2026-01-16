import pytest
from unittest.mock import Mock, patch
from framework.service_entrypoint import ServiceEntrypoint


class TestServiceEntrypoint:

    def test_init_with_interceptor_pipeline(self):
        """Test initialization with provided interceptor pipeline."""
        registry = Mock()
        pipeline = Mock()

        entrypoint = ServiceEntrypoint(registry, interceptor_pipeline=pipeline)

        assert entrypoint.registry == registry
        assert entrypoint.interceptor_pipeline == pipeline

    @patch('framework.service_entrypoint.InterceptorPipeline')
    @patch('framework.service_entrypoint.InterceptorRegistry')
    def test_init_with_interceptor_config(self, mock_interceptor_registry, mock_pipeline):
        """Test initialization with interceptor configuration file."""
        registry = Mock()
        mock_pipeline_instance = Mock()
        mock_registry_instance = Mock()
        mock_interceptor1 = Mock()
        mock_interceptor2 = Mock()

        mock_pipeline.return_value = mock_pipeline_instance
        mock_interceptor_registry.return_value = mock_registry_instance
        mock_registry_instance.get_enabled_interceptors.return_value = [mock_interceptor1, mock_interceptor2]

        entrypoint = ServiceEntrypoint(registry, interceptor_config_path="test_interceptors.json")

        assert entrypoint.registry == registry
        assert entrypoint.interceptor_pipeline == mock_pipeline_instance
        mock_interceptor_registry.assert_called_once_with("test_interceptors.json")
        mock_registry_instance.get_enabled_interceptors.assert_called_once()
        mock_pipeline_instance.add_interceptor.assert_any_call(mock_interceptor1)
        mock_pipeline_instance.add_interceptor.assert_any_call(mock_interceptor2)

    @patch('framework.service_entrypoint.InterceptorPipeline')
    def test_init_no_interceptor_config(self, mock_pipeline):
        """Test initialization with no interceptor configuration."""
        registry = Mock()
        mock_pipeline_instance = Mock()
        mock_pipeline.return_value = mock_pipeline_instance

        entrypoint = ServiceEntrypoint(registry, interceptor_config_path=None)

        assert entrypoint.registry == registry
        assert entrypoint.interceptor_pipeline == mock_pipeline_instance

    @patch('framework.service_entrypoint.InterceptorPipeline')
    @patch('framework.service_entrypoint.InterceptorRegistry')
    def test_init_interceptor_config_error(self, mock_interceptor_registry, mock_pipeline):
        """Test initialization when interceptor configuration fails."""
        registry = Mock()
        mock_pipeline_instance = Mock()
        mock_pipeline.return_value = mock_pipeline_instance
        mock_interceptor_registry.side_effect = Exception("Config file not found")

        entrypoint = ServiceEntrypoint(registry, interceptor_config_path="invalid.json")

        # Should still initialize with empty pipeline
        assert entrypoint.registry == registry
        assert entrypoint.interceptor_pipeline == mock_pipeline_instance

    def test_execute_success(self):
        """Test successful service execution."""
        registry = Mock()
        pipeline = Mock()
        executor = Mock()

        registry.get_executor.return_value = executor
        executor.execute.return_value = {"result": "success"}
        pipeline.execute.return_value = {"result": "success", "interceptor": "processed"}

        entrypoint = ServiceEntrypoint(registry, interceptor_pipeline=pipeline)
        context = {"service_id": "test_service", "data": "input"}

        result = entrypoint.execute(context)

        assert result == {"result": "success", "interceptor": "processed"}
        registry.get_executor.assert_called_once_with("test_service")
        pipeline.execute.assert_called_once()

    def test_execute_missing_service_id(self):
        """Test execution with missing service_id in context."""
        registry = Mock()
        pipeline = Mock()

        entrypoint = ServiceEntrypoint(registry, interceptor_pipeline=pipeline)
        context = {"data": "input"}

        with pytest.raises(KeyError, match="'service_id' is required in the context"):
            entrypoint.execute(context)

    def test_execute_service_not_found(self):
        """Test execution with non-existent service."""
        registry = Mock()
        pipeline = Mock()

        registry.get_executor.side_effect = KeyError("Service 'nonexistent' not found")

        entrypoint = ServiceEntrypoint(registry, interceptor_pipeline=pipeline)
        context = {"service_id": "nonexistent"}

        with pytest.raises(KeyError, match="Service 'nonexistent' not found"):
            entrypoint.execute(context)

    def test_execute_service_execution_error(self):
        """Test execution when service throws an exception."""
        registry = Mock()
        pipeline = Mock()
        executor = Mock()

        registry.get_executor.return_value = executor
        pipeline.execute.side_effect = RuntimeError("Service execution failed")

        entrypoint = ServiceEntrypoint(registry, interceptor_pipeline=pipeline)
        context = {"service_id": "failing_service"}

        with pytest.raises(RuntimeError, match="Service execution failed"):
            entrypoint.execute(context)

    def test_executor_wrapper_functionality(self):
        """Test that the ExecutorWrapper works correctly."""
        registry = Mock()
        pipeline = Mock()
        executor = Mock()

        registry.get_executor.return_value = executor
        executor.execute.return_value = {"step": "result"}

        def pipeline_execute(context, wrapper):
            # Simulate interceptor calling the wrapper
            return wrapper.execute(context)

        pipeline.execute.side_effect = pipeline_execute

        entrypoint = ServiceEntrypoint(registry, interceptor_pipeline=pipeline)
        context = {"service_id": "test_service", "data": "input"}

        result = entrypoint.execute(context)

        assert result == {"step": "result"}
        executor.execute.assert_called_once_with(context)

    @patch('framework.service_entrypoint.InterceptorPipeline')
    @patch('framework.service_entrypoint.InterceptorRegistry')
    def test_build_pipeline_success(self, mock_interceptor_registry, mock_pipeline):
        """Test successful pipeline building."""
        registry = Mock()
        mock_pipeline_instance = Mock()
        mock_registry_instance = Mock()
        mock_interceptor1 = Mock()
        mock_interceptor2 = Mock()

        mock_pipeline.return_value = mock_pipeline_instance
        mock_interceptor_registry.return_value = mock_registry_instance
        mock_registry_instance.get_enabled_interceptors.return_value = [mock_interceptor1, mock_interceptor2]

        entrypoint = ServiceEntrypoint(registry, interceptor_config_path="interceptors.json")

        # Verify pipeline was built correctly
        mock_interceptor_registry.assert_called_once_with("interceptors.json")
        mock_pipeline_instance.add_interceptor.assert_any_call(mock_interceptor1)
        mock_pipeline_instance.add_interceptor.assert_any_call(mock_interceptor2)

    @patch('framework.service_entrypoint.InterceptorPipeline')
    def test_build_pipeline_no_config(self, mock_pipeline):
        """Test pipeline building with no configuration file."""
        registry = Mock()
        mock_pipeline_instance = Mock()
        mock_pipeline.return_value = mock_pipeline_instance

        entrypoint = ServiceEntrypoint(registry, interceptor_config_path=None)

        # Should create empty pipeline
        mock_pipeline.assert_called_once()
        mock_pipeline_instance.add_interceptor.assert_not_called()

    def test_multiple_service_executions(self):
        """Test multiple service executions with same entrypoint."""
        registry = Mock()
        pipeline = Mock()
        executor1 = Mock()
        executor2 = Mock()

        def get_executor_side_effect(service_id):
            if service_id == "service1":
                return executor1
            elif service_id == "service2":
                return executor2

        registry.get_executor.side_effect = get_executor_side_effect
        executor1.execute.return_value = {"result": "service1_result"}
        executor2.execute.return_value = {"result": "service2_result"}

        def pipeline_execute(context, wrapper):
            return wrapper.execute(context)

        pipeline.execute.side_effect = pipeline_execute

        entrypoint = ServiceEntrypoint(registry, interceptor_pipeline=pipeline)

        # Execute first service
        result1 = entrypoint.execute({"service_id": "service1"})
        assert result1 == {"result": "service1_result"}

        # Execute second service
        result2 = entrypoint.execute({"service_id": "service2"})
        assert result2 == {"result": "service2_result"}

        # Verify both executors were called
        executor1.execute.assert_called_once()
        executor2.execute.assert_called_once()
