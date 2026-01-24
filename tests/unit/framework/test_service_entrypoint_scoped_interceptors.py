from unittest.mock import Mock, patch

from framework.service_entrypoint import ServiceEntrypoint


class TestServiceEntrypointScopedInterceptors:

    @patch('framework.service_entrypoint.InterceptorPipeline')
    @patch('framework.service_entrypoint.InterceptorRegistry')
    def test_scoped_pipelines_cached_per_service(self, mock_registry_cls, mock_pipeline_cls):
        registry = Mock()
        executor = Mock()
        registry.get_executor.return_value = executor
        executor.execute.return_value = {"result": "ok"}

        pipeline_instance = Mock()
        mock_pipeline_cls.side_effect = [pipeline_instance, Mock()]

        interceptor_registry = Mock()
        interceptor_registry.get_enabled_interceptors_for_service.return_value = []
        mock_registry_cls.return_value = interceptor_registry

        entrypoint = ServiceEntrypoint(registry, interceptor_config_path="interceptors.json")

        pipeline_instance.execute.return_value = {"result": "ok"}

        context = {"service_id": "service-a"}
        entrypoint.execute(context)
        entrypoint.execute(context)

        assert interceptor_registry.get_enabled_interceptors_for_service.call_count == 1

    @patch('framework.service_entrypoint.InterceptorPipeline')
    @patch('framework.service_entrypoint.InterceptorRegistry')
    def test_scoped_pipelines_differ_by_service(self, mock_registry_cls, mock_pipeline_cls):
        registry = Mock()
        executor = Mock()
        registry.get_executor.return_value = executor
        executor.execute.return_value = {"result": "ok"}

        base_pipeline = Mock()
        pipeline_instances = [Mock(), Mock()]
        mock_pipeline_cls.side_effect = [base_pipeline, *pipeline_instances]

        interceptor_registry = Mock()
        interceptor_registry.get_enabled_interceptors_for_service.return_value = []
        mock_registry_cls.return_value = interceptor_registry

        entrypoint = ServiceEntrypoint(registry, interceptor_config_path="interceptors.json")

        base_pipeline.execute.return_value = {"result": "ok"}
        pipeline_instances[0].execute.return_value = {"result": "ok"}
        pipeline_instances[1].execute.return_value = {"result": "ok"}

        entrypoint.execute({"service_id": "service-a"})
        entrypoint.execute({"service_id": "service-b"})

        assert interceptor_registry.get_enabled_interceptors_for_service.call_count == 2
        assert mock_pipeline_cls.call_count == 3

    def test_explicit_pipeline_bypasses_scoping(self):
        registry = Mock()
        executor = Mock()
        registry.get_executor.return_value = executor

        pipeline = Mock()
        pipeline.execute.return_value = {"result": "ok"}

        entrypoint = ServiceEntrypoint(registry, interceptor_pipeline=pipeline)

        result = entrypoint.execute({"service_id": "service-a"})
        assert result == {"result": "ok"}
        pipeline.execute.assert_called_once()
