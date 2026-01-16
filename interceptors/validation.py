import logging
from typing import Dict, Any, List, Optional
from framework.interceptor import Interceptor


class ValidationInterceptor(Interceptor):
    """Interceptor for request and response validation"""

    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the validation interceptor

        Args:
            config: Configuration dictionary with options:
                - validate_request: Whether to validate requests
                - validate_response: Whether to validate responses
                - strict_mode: Fail on validation errors vs warnings
                - required_fields: List of required fields in context
                - max_payload_size: Maximum payload size in bytes
        """
        self.config = config or {}
        self.validate_request = self.config.get('validate_request', True)
        self.validate_response = self.config.get('validate_response', False)
        self.strict_mode = self.config.get('strict_mode', False)
        self.required_fields = self.config.get('required_fields', ['service_id'])
        self.max_payload_size = self.config.get('max_payload_size', 1024 * 1024)  # 1MB default

        self.logger = logging.getLogger(__name__)

    def before(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate request context

        Args:
            context: The request context

        Returns:
            The context if valid

        Raises:
            ValidationError: If validation fails in strict mode
        """
        if self.validate_request:
            validation_errors = self._validate_request(context)
            if validation_errors:
                if self.strict_mode:
                    raise ValidationError(f"Request validation failed: {validation_errors}")
                else:
                    for error in validation_errors:
                        self.logger.warning(f"Request validation warning: {error}")

        return context

    def after(self, context: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate response

        Args:
            context: The request context
            result: The result from the component execution

        Returns:
            The result if valid

        Raises:
            ValidationError: If validation fails in strict mode
        """
        if self.validate_response:
            validation_errors = self._validate_response(result)
            if validation_errors:
                if self.strict_mode:
                    raise ValidationError(f"Response validation failed: {validation_errors}")
                else:
                    for error in validation_errors:
                        self.logger.warning(f"Response validation warning: {error}")

        return result

    def _validate_request(self, context: Dict[str, Any]) -> List[str]:
        """
        Validate the request context

        Args:
            context: Request context to validate

        Returns:
            List of validation errors (empty if valid)
        """
        errors = []

        # Check required fields
        for field in self.required_fields:
            if field not in context:
                errors.append(f"Missing required field: {field}")

        # Validate service_id format
        if 'service_id' in context:
            service_id = context['service_id']
            if not isinstance(service_id, str) or not service_id:
                errors.append("Invalid service_id: must be non-empty string")
            elif not self._is_valid_service_id(service_id):
                errors.append(f"Invalid service_id format: {service_id}")

        # Validate request_id if present
        if 'request_id' in context:
            request_id = context['request_id']
            if not isinstance(request_id, str) or not request_id:
                errors.append("Invalid request_id: must be non-empty string")

        # Check payload size
        if 'data' in context:
            payload_size = self._estimate_size(context['data'])
            if payload_size > self.max_payload_size:
                errors.append(
                    f"Payload too large: {payload_size} bytes "
                    f"(max: {self.max_payload_size} bytes)"
                )

        # Validate data structure
        if 'data' in context and context['data'] is not None:
            if not isinstance(context['data'], (dict, list, str, int, float, bool)):
                errors.append("Invalid data type: must be JSON-serializable")

        return errors

    def _validate_response(self, response: Dict[str, Any]) -> List[str]:
        """
        Validate the response

        Args:
            response: Response to validate

        Returns:
            List of validation errors (empty if valid)
        """
        errors = []

        # Check if response is a dictionary
        if not isinstance(response, dict):
            errors.append("Response must be a dictionary")
            return errors

        # Check response size
        response_size = self._estimate_size(response)
        if response_size > self.max_payload_size * 2:  # Allow larger responses
            errors.append(
                f"Response too large: {response_size} bytes "
                f"(max: {self.max_payload_size * 2} bytes)"
            )

        # Check for error indicators
        if 'error' in response and response['error']:
            if 'error_code' not in response:
                errors.append("Response with error should include error_code")
            if 'error_message' not in response:
                errors.append("Response with error should include error_message")

        return errors

    def _is_valid_service_id(self, service_id: str) -> bool:
        """
        Check if service_id follows valid format

        Args:
            service_id: Service ID to validate

        Returns:
            True if valid format
        """
        # Service ID should contain only alphanumeric, dash, and underscore
        import re
        pattern = r'^[a-zA-Z0-9\-_]+$'
        return bool(re.match(pattern, service_id))

    def _estimate_size(self, obj: Any) -> int:
        """
        Estimate the size of an object in bytes

        Args:
            obj: Object to estimate size for

        Returns:
            Estimated size in bytes
        """
        import json
        try:
            return len(json.dumps(obj).encode('utf-8'))
        except (TypeError, ValueError):
            # If not JSON serializable, estimate based on string representation
            return len(str(obj).encode('utf-8'))


class ValidationError(Exception):
    """Exception raised for validation failures"""
    pass
