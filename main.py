#!/usr/bin/env python3
"""
Main entry point for the Model Service Framework
Example usage and demonstration
"""

import logging
import sys
from framework import ServiceRegistry, ServiceEntrypoint, MiddlewarePipeline
from framework.logging_middleware import LoggingMiddleware


def setup_logging():
    """Configure logging for the application"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def main():
    """Main function demonstrating framework usage"""
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize the service registry with configuration
        logger.info("Initializing service registry...")
        registry = ServiceRegistry(config_path='services.json')
        
        # Create middleware pipeline
        logger.info("Setting up middleware pipeline...")
        middleware_pipeline = MiddlewarePipeline()
        
        # Add logging middleware
        logging_middleware = LoggingMiddleware(logger)
        middleware_pipeline.add_middleware(logging_middleware)
        
        # Create service entrypoint
        logger.info("Creating service entrypoint...")
        service = ServiceEntrypoint(
            registry=registry,
            middleware_pipeline=middleware_pipeline
        )
        
        # Example 1: Execute Pre-Calibration Component
        logger.info("\n=== Example 1: Pre-Calibration Component ===")
        pre_calibration_context = {
            "service_id": "pre-calibration",
            "request_id": "req-001",
            "data": {
                "temperature": 25.0,
                "pressure": 101.325
            }
        }
        
        result1 = service.execute(pre_calibration_context)
        logger.info(f"Pre-Calibration Result: {result1}")
        
        # Example 2: Execute Simulation Component
        logger.info("\n=== Example 2: Simulation Component ===")
        simulation_context = {
            "service_id": "simulation",
            "request_id": "req-002",
            "data": {
                "model": "physics-v1",
                "parameters": {
                    "time_step": 0.01,
                    "duration": 10.0
                }
            }
        }
        
        result2 = service.execute(simulation_context)
        logger.info(f"Simulation Result: {result2}")
        
        # Example 3: Handle missing service_id
        logger.info("\n=== Example 3: Error Handling - Missing service_id ===")
        try:
            invalid_context = {
                "request_id": "req-003",
                "data": {}
            }
            service.execute(invalid_context)
        except KeyError as e:
            logger.error(f"Expected error: {e}")
        
        # Example 4: Handle unknown service
        logger.info("\n=== Example 4: Error Handling - Unknown service ===")
        try:
            unknown_service_context = {
                "service_id": "unknown-service",
                "request_id": "req-004"
            }
            service.execute(unknown_service_context)
        except KeyError as e:
            logger.error(f"Expected error: {e}")
        
        # List all registered services
        logger.info("\n=== Registered Services ===")
        services = registry.list_services()
        for service_id, module_path in services.items():
            logger.info(f"  {service_id}: {module_path}")
        
    except Exception as e:
        logger.error(f"Application error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()