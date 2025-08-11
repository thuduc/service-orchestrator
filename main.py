#!/usr/bin/env python3
"""
Main entry point for the Model Service Framework
Example usage and demonstration of steps-based architecture
"""

import logging
import sys
from framework import ServiceRegistry, ServiceEntrypoint, MiddlewarePipeline
from framework.logging_middleware import LoggingMiddleware
from framework.config_validator import validate_config


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
        # Validate configuration first
        logger.info("Validating service configuration...")
        if not validate_config('services.json'):
            logger.error("Configuration validation failed")
            sys.exit(1)
        
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
        
        # Display registered services
        logger.info("\n=== Registered Services ===")
        services = registry.list_services()
        for service_id, num_steps in services.items():
            logger.info(f"  {service_id}: {num_steps} step(s)")
            # Get detailed service info
            info = registry.get_service_info(service_id)
            for step in info['steps']:
                logger.info(f"    - Step '{step['name']}': {step['module']}.{step['class']}")
        
        # Example 1: Execute Pre-Calibration Service (single-step)
        logger.info("\n=== Example 1: Pre-Calibration Service (Single-Step) ===")
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
        
        # Example 2: Execute Simulation Service (single-step)
        logger.info("\n=== Example 2: Simulation Service (Single-Step) ===")
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
        
        # Example 3: Execute Data Processing Service (multi-step)
        logger.info("\n=== Example 3: Data Processing Service (Multi-Step) ===")
        data_processing_context = {
            "service_id": "data-processing",
            "request_id": "req-003",
            "data": {
                "name": "test data",
                "value": 42,
                "description": "sample data for processing"
            }
        }
        
        result3 = service.execute(data_processing_context)
        logger.info(f"Data Processing Result: {result3}")
        
        # Example 4: Test validation failure in multi-step service
        logger.info("\n=== Example 4: Data Processing with Validation Failure ===")
        invalid_data_context = {
            "service_id": "data-processing",
            # Missing required 'request_id' field
            "data": {
                "name": "incomplete data"
            }
        }
        
        try:
            result4 = service.execute(invalid_data_context)
            logger.info(f"Result: {result4}")
        except Exception as e:
            logger.error(f"Processing failed as expected: {e}")
        
        # Example 5: Handle missing service_id
        logger.info("\n=== Example 5: Error Handling - Missing service_id ===")
        try:
            invalid_context = {
                "request_id": "req-005",
                "data": {}
            }
            service.execute(invalid_context)
        except KeyError as e:
            logger.error(f"Expected error: {e}")
        
        # Example 6: Handle unknown service
        logger.info("\n=== Example 6: Error Handling - Unknown service ===")
        try:
            unknown_service_context = {
                "service_id": "unknown-service",
                "request_id": "req-006"
            }
            service.execute(unknown_service_context)
        except KeyError as e:
            logger.error(f"Expected error: {e}")
        
        logger.info("\n=== All tests completed successfully ===")
        
    except Exception as e:
        logger.error(f"Application error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()