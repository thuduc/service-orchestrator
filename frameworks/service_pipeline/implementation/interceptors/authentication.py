import logging
from typing import Dict, Any, Optional
from ...contract import Interceptor


class AuthenticationInterceptor(Interceptor):
    """Interceptor for authentication and authorization"""

    def __init__(self, config: Dict[str, Any] | None = None):
        """
        Initialize the authentication interceptor

        Args:
            config: Configuration dictionary with options:
                - auth_type: Type of authentication (bearer, api_key, basic)
                - validate_token: Whether to validate tokens
                - bypass_services: List of service IDs to bypass auth
                - required_headers: List of required auth headers
        """
        self.config = config or {}
        self.auth_type = self.config.get('auth_type', 'bearer')
        self.validate_token = self.config.get('validate_token', True)
        self.bypass_services = self.config.get('bypass_services', [])
        self.required_headers = self.config.get('required_headers', ['Authorization'])

        self.logger = logging.getLogger(__name__)

    def before(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate authentication before processing request

        Args:
            context: The request context

        Returns:
            The context with authenticated user info

        Raises:
            AuthenticationError: If authentication fails
        """
        service_id = context.get('service_id', '')

        # Check if service should bypass authentication
        if service_id in self.bypass_services:
            self.logger.debug(f"Bypassing authentication for service: {service_id}")
            return context

        # Extract authentication information
        auth_info = context.get('auth', {})
        headers = context.get('headers', {})

        # Validate based on auth type
        if self.auth_type == 'bearer':
            if not self._validate_bearer_token(auth_info, headers):
                raise AuthenticationError("Invalid or missing bearer token")
        elif self.auth_type == 'api_key':
            if not self._validate_api_key(auth_info, headers):
                raise AuthenticationError("Invalid or missing API key")
        elif self.auth_type == 'basic':
            if not self._validate_basic_auth(auth_info, headers):
                raise AuthenticationError("Invalid or missing basic authentication")

        # Add authenticated user info to context
        if 'user' not in context and auth_info.get('user'):
            context['authenticated_user'] = auth_info['user']

        self.logger.info(f"Authentication successful for service: {service_id}")

        return context

    def _validate_bearer_token(self, auth_info: Dict[str, Any],
                               headers: Dict[str, str]) -> bool:
        """Validate bearer token authentication"""
        if not self.validate_token:
            return True

        # Check for Authorization header
        auth_header = headers.get('Authorization', '')
        if not auth_header.startswith('Bearer '):
            # Check if token is in auth_info
            token = auth_info.get('token')
            if not token:
                self.logger.warning("Missing bearer token")
                return False
        else:
            token = auth_header[7:]  # Remove 'Bearer ' prefix

        # In a real implementation, validate token against auth service
        # For now, just check if token exists and has minimum length
        if len(token) < 10:
            self.logger.warning("Invalid bearer token format")
            return False

        # Mock validation - in production, verify with auth service
        return self._mock_validate_token(token)

    def _validate_api_key(self, auth_info: Dict[str, Any],
                          headers: Dict[str, str]) -> bool:
        """Validate API key authentication"""
        api_key = headers.get('X-API-Key') or auth_info.get('api_key')

        if not api_key:
            self.logger.warning("Missing API key")
            return False

        # Mock validation - in production, verify against stored keys
        return len(api_key) >= 20

    def _validate_basic_auth(self, auth_info: Dict[str, Any],
                             headers: Dict[str, str]) -> bool:
        """Validate basic authentication"""
        auth_header = headers.get('Authorization', '')

        if not auth_header.startswith('Basic '):
            # Check if credentials are in auth_info
            username = auth_info.get('username')
            password = auth_info.get('password')
            if not username or not password:
                self.logger.warning("Missing basic auth credentials")
                return False
        else:
            # In production, decode and validate basic auth header
            pass

        return True

    def _mock_validate_token(self, token: str) -> bool:
        """
        Mock token validation
        In production, this would validate against an auth service
        """
        # For demo purposes, tokens starting with 'valid' are considered valid
        return token.startswith('valid') or token == 'test-token'


class AuthenticationError(Exception):
    """Exception raised for authentication failures"""
    pass
