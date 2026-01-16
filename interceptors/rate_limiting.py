import logging
import time
from typing import Dict, Any, Deque, Optional
from collections import defaultdict, deque
from framework.interceptor import Interceptor


class RateLimitingInterceptor(Interceptor):
    """Interceptor for rate limiting requests"""

    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the rate limiting interceptor

        Args:
            config: Configuration dictionary with options:
                - requests_per_minute: Maximum requests per minute per client
                - burst_size: Maximum burst size allowed
                - rate_limit_by: Field to use for rate limiting (client_id, ip, user)
                - exclude_services: List of service IDs to exclude from rate limiting
        """
        self.config = config or {}
        self.requests_per_minute = self.config.get('requests_per_minute', 100)
        self.burst_size = self.config.get('burst_size', 10)
        self.rate_limit_by = self.config.get('rate_limit_by', 'client_id')
        self.exclude_services = self.config.get('exclude_services', [])

        # Calculate rate limit parameters
        self.window_size = 60  # 1 minute window
        self.max_requests = self.requests_per_minute

        # Storage for request tracking
        # Format: {client_id: deque of timestamps}
        self.request_history: Dict[str, Deque[float]] = defaultdict(lambda: deque())

        self.logger = logging.getLogger(__name__)

    def before(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply rate limiting before processing request

        Args:
            context: The request context

        Returns:
            The context if within rate limit

        Raises:
            RateLimitExceeded: If rate limit is exceeded
        """
        service_id = context.get('service_id', '')

        # Check if service should be excluded from rate limiting
        if service_id in self.exclude_services:
            self.logger.debug(f"Skipping rate limiting for service: {service_id}")
            return context

        # Determine client identifier for rate limiting
        client_id = self._get_client_id(context)
        if not client_id:
            # If no client ID can be determined, skip rate limiting
            self.logger.warning("Cannot determine client ID for rate limiting")
            return context

        # Check rate limit
        current_time = time.time()
        if not self._check_rate_limit(client_id, current_time):
            self.logger.warning(f"Rate limit exceeded for client: {client_id}")
            raise RateLimitExceeded(
                f"Rate limit exceeded: {self.requests_per_minute} requests per minute"
            )

        # Record this request
        self._record_request(client_id, current_time)

        # Store client_id for after() to use
        context['_rate_limit_client_id'] = client_id
        context['_rate_limit_time'] = current_time

        return context

    def after(self, context: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add rate limit info to response

        Args:
            context: The request context
            result: The result from the component execution

        Returns:
            The result with rate limit info added
        """
        client_id = context.get('_rate_limit_client_id')
        current_time = context.get('_rate_limit_time', time.time())

        # Clean up context
        if '_rate_limit_client_id' in context:
            del context['_rate_limit_client_id']
        if '_rate_limit_time' in context:
            del context['_rate_limit_time']

        # Add rate limit info to response
        if client_id and isinstance(result, dict):
            result['rate_limit_info'] = self._get_rate_limit_info(client_id, current_time)

        return result

    def _get_client_id(self, context: Dict[str, Any]) -> str:
        """
        Extract client identifier from context

        Args:
            context: Request context

        Returns:
            Client identifier string or empty string if not found
        """
        # Try different fields based on configuration
        if self.rate_limit_by == 'client_id':
            return context.get('client_id', '')
        elif self.rate_limit_by == 'ip':
            return context.get('client_ip', context.get('ip_address', ''))
        elif self.rate_limit_by == 'user':
            return context.get('user_id', context.get('authenticated_user', ''))
        else:
            # Default to any available identifier
            return (context.get('client_id') or
                    context.get('user_id') or
                    context.get('client_ip') or
                    context.get('request_id', ''))

    def _check_rate_limit(self, client_id: str, current_time: float) -> bool:
        """
        Check if request is within rate limit

        Args:
            client_id: Client identifier
            current_time: Current timestamp

        Returns:
            True if within limit, False otherwise
        """
        # Get request history for this client
        history = self.request_history[client_id]

        # Remove old requests outside the window
        window_start = current_time - self.window_size
        while history and history[0] < window_start:
            history.popleft()

        # Check if we're within the limit
        if len(history) >= self.max_requests:
            return False

        # Check burst limit (requests in last few seconds)
        burst_window = 5  # 5 second burst window
        burst_start = current_time - burst_window
        burst_count = sum(1 for t in history if t > burst_start)
        if burst_count >= self.burst_size:
            return False

        return True

    def _record_request(self, client_id: str, timestamp: float):
        """
        Record a request for rate limiting

        Args:
            client_id: Client identifier
            timestamp: Request timestamp
        """
        self.request_history[client_id].append(timestamp)

    def _get_rate_limit_info(self, client_id: str, current_time: float) -> Dict[str, Any]:
        """
        Get current rate limit information for client

        Args:
            client_id: Client identifier
            current_time: Current timestamp

        Returns:
            Dictionary with rate limit information
        """
        history = self.request_history[client_id]

        # Clean old entries
        window_start = current_time - self.window_size
        valid_requests = [t for t in history if t > window_start]

        remaining = max(0, self.max_requests - len(valid_requests))
        reset_time = window_start + self.window_size if valid_requests else current_time + self.window_size

        return {
            'limit': self.max_requests,
            'remaining': remaining,
            'reset': int(reset_time),
            'window': self.window_size
        }

    def reset_client(self, client_id: str):
        """
        Reset rate limit for a specific client

        Args:
            client_id: Client identifier to reset
        """
        if client_id in self.request_history:
            del self.request_history[client_id]
            self.logger.info(f"Reset rate limit for client: {client_id}")

    def reset_all(self):
        """Reset rate limits for all clients"""
        self.request_history.clear()
        self.logger.info("Reset all rate limits")


class RateLimitExceeded(Exception):
    """Exception raised when rate limit is exceeded"""
    pass
