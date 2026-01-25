from typing import Dict, Any, Optional
from .service_registry import ServiceRegistry
from .interceptor_pipeline import InterceptorPipeline
from .interceptor_registry import InterceptorRegistry
import logging

logger = logging.getLogger(__name__)


class ServiceEntrypoint:
    """Main microservice entrypoint for steps-based services"""

    def __init__(self, registry: ServiceRegistry,
                 interceptor_pipeline: Optional[InterceptorPipeline] = None,
                 interceptor_config_path: Optional[str] = 'interceptors.json'):
        """
        Initialize the service entrypoint

        Args:
            registry: The service registry for executor lookup
            interceptor_pipeline: Optional pre-configured interceptor pipeline
            interceptor_config_path: Path to interceptor configuration file
        """
        self.registry = registry
        self._interceptor_config_path = interceptor_config_path
        self._interceptor_registry: Optional[InterceptorRegistry] = None
        self._pipeline_cache: Dict[str, InterceptorPipeline] = {}

        # Use provided pipeline or prepare for interceptor config loading
        if interceptor_pipeline:
            self.interceptor_pipeline = interceptor_pipeline
            self._interceptor_config_path = None
        else:
            self.interceptor_pipeline = InterceptorPipeline()
            if interceptor_config_path:
                try:
                    self._interceptor_registry = InterceptorRegistry(interceptor_config_path)
                except Exception as e:
                    logger.warning(f"Failed to load interceptor configuration: {e}")
                    self._interceptor_config_path = None
            else:
                self._interceptor_config_path = None

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main execution method with interceptor support

        Args:
            context: Request context containing at minimum 'service_id'

        Returns:
            Response dictionary from the service execution

        Raises:
            KeyError: If 'service_id' is not in context
            Exception: Any exception from service execution
        """
        # Extract service_id from context
        if 'service_id' not in context:
            raise KeyError("'service_id' is required in the context")

        service_id = context['service_id']
        logger.info(f"Executing service: {service_id}")

        try:
            # Get steps executor from registry
            executor = self.registry.get_executor(service_id)

            # Create a wrapper to make executor compatible with interceptor pipeline
            class ExecutorWrapper:
                def __init__(self, steps_executor):
                    self._executor = steps_executor

                def execute(self, ctx):
                    return self._executor.execute(ctx)

            wrapper = ExecutorWrapper(executor)

            # Execute through interceptor pipeline
            pipeline = self._get_pipeline_for_service(service_id)
            result = pipeline.execute(context, wrapper)  # type: ignore[arg-type]

            logger.info(f"Service '{service_id}' executed successfully")
            return result

        except KeyError as e:
            logger.error(f"Service not found: {e}")
            raise
        except Exception as e:
            logger.error(f"Service execution failed: {e}")
            raise

    def _get_pipeline_for_service(self, service_id: str) -> InterceptorPipeline:
        """
        Get or build the interceptor pipeline for a specific service.

        Args:
            service_id: Service identifier to use for interceptor scoping

        Returns:
            Interceptor pipeline scoped to the service
        """
        if self._interceptor_config_path is None or self._interceptor_registry is None:
            return self.interceptor_pipeline

        if service_id in self._pipeline_cache:
            return self._pipeline_cache[service_id]

        pipeline = InterceptorPipeline()
        interceptors = self._interceptor_registry.get_enabled_interceptors_for_service(service_id)
        for interceptor in interceptors:
            pipeline.add_interceptor(interceptor)

        self._pipeline_cache[service_id] = pipeline
        return pipeline
