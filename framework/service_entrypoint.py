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

        # Use provided pipeline or create new one with auto-registration
        if interceptor_pipeline:
            self.interceptor_pipeline = interceptor_pipeline
        else:
            self.interceptor_pipeline = self._build_pipeline(interceptor_config_path)

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
            result = self.interceptor_pipeline.execute(context, wrapper)

            logger.info(f"Service '{service_id}' executed successfully")
            return result

        except KeyError as e:
            logger.error(f"Service not found: {e}")
            raise
        except Exception as e:
            logger.error(f"Service execution failed: {e}")
            raise

    def _build_pipeline(self, interceptor_config_path: Optional[str]) -> InterceptorPipeline:
        """
        Build interceptor pipeline from configuration

        Args:
            interceptor_config_path: Path to interceptor configuration file

        Returns:
            Configured interceptor pipeline
        """
        pipeline = InterceptorPipeline()

        if interceptor_config_path:
            try:
                # Create interceptor registry and load configuration
                interceptor_registry = InterceptorRegistry(interceptor_config_path)

                # Get all enabled interceptors sorted by order
                interceptors = interceptor_registry.get_enabled_interceptors()

                # Add each interceptor to the pipeline
                for interceptor in interceptors:
                    pipeline.add_interceptor(interceptor)

                logger.info(f"Loaded {len(interceptors)} interceptor(s) from {interceptor_config_path}")
            except Exception as e:
                logger.warning(f"Failed to load interceptor configuration: {e}")

        return pipeline
