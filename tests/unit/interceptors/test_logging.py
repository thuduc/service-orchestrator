import pytest
import logging
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock
from interceptors.logging import LoggingInterceptor, ContextLogger


class TestContextLogger:

    def test_init(self):
        """Test ContextLogger initialization."""
        base_logger = Mock()
        context = {"service_id": "test_service", "request_id": "req_123"}

        logger = ContextLogger(base_logger, context)

        assert logger.base_logger == base_logger
        assert logger.context == context
        assert logger.service_id == "test_service"
        assert logger.request_id == "req_123"

    def test_init_missing_context_fields(self):
        """Test initialization with missing context fields."""
        base_logger = Mock()
        context = {}

        logger = ContextLogger(base_logger, context)

        assert logger.service_id == "unknown"
        assert logger.request_id == "N/A"

    def test_format_message(self):
        """Test message formatting with context."""
        base_logger = Mock()
        context = {"service_id": "auth_service", "request_id": "auth_req_456"}

        logger = ContextLogger(base_logger, context)
        formatted = logger._format_message("Test message")

        expected = "[Service: auth_service, Request: auth_req_456] Test message"
        assert formatted == expected

    def test_debug_logging(self):
        """Test debug logging with context."""
        base_logger = Mock()
        context = {"service_id": "debug_service", "request_id": "debug_123"}

        logger = ContextLogger(base_logger, context)
        logger.debug("Debug message")

        expected_message = "[Service: debug_service, Request: debug_123] Debug message"
        base_logger.debug.assert_called_once_with(expected_message)

    def test_info_logging(self):
        """Test info logging with context."""
        base_logger = Mock()
        context = {"service_id": "info_service", "request_id": "info_456"}

        logger = ContextLogger(base_logger, context)
        logger.info("Info message")

        expected_message = "[Service: info_service, Request: info_456] Info message"
        base_logger.info.assert_called_once_with(expected_message)

    def test_warning_logging(self):
        """Test warning logging with context."""
        base_logger = Mock()
        context = {"service_id": "warn_service", "request_id": "warn_789"}

        logger = ContextLogger(base_logger, context)
        logger.warning("Warning message")

        expected_message = "[Service: warn_service, Request: warn_789] Warning message"
        base_logger.warning.assert_called_once_with(expected_message)

    def test_error_logging(self):
        """Test error logging with context."""
        base_logger = Mock()
        context = {"service_id": "error_service", "request_id": "error_000"}

        logger = ContextLogger(base_logger, context)
        logger.error("Error message")

        expected_message = "[Service: error_service, Request: error_000] Error message"
        base_logger.error.assert_called_once_with(expected_message)

    def test_critical_logging(self):
        """Test critical logging with context."""
        base_logger = Mock()
        context = {"service_id": "critical_service", "request_id": "critical_999"}

        logger = ContextLogger(base_logger, context)
        logger.critical("Critical message")

        expected_message = "[Service: critical_service, Request: critical_999] Critical message"
        base_logger.critical.assert_called_once_with(expected_message)

    def test_logging_with_args_and_kwargs(self):
        """Test logging with additional args and kwargs."""
        base_logger = Mock()
        context = {"service_id": "args_service", "request_id": "args_123"}

        logger = ContextLogger(base_logger, context)
        logger.info("Message with %s and %d", "args", 42, extra={"key": "value"})

        expected_message = "[Service: args_service, Request: args_123] Message with %s and %d"
        base_logger.info.assert_called_once_with(expected_message, "args", 42, extra={"key": "value"})


class TestLoggingInterceptor:

    def test_init_default_config(self):
        """Test initialization with default configuration."""
        interceptor = LoggingInterceptor()

        assert interceptor.config == {}
        assert interceptor.log_level == logging.INFO
        assert interceptor.log_request is True
        assert interceptor.log_response is True
        assert interceptor.log_errors is True
        assert interceptor.provide_context_logger is True
        assert interceptor.destinations == ['stdout']
        assert interceptor.file_path == 'logs/service.log'

    def test_init_custom_config(self):
        """Test initialization with custom configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_path = os.path.join(temp_dir, "custom.log")
            config = {
                "log_level": "DEBUG",
                "log_request": False,
                "log_response": False,
                "log_errors": False,
                "provide_context_logger": False,
                "destinations": ["file"],
                "file_path": log_path,
                "log_format": "Custom format: %(message)s",
                "date_format": "%Y-%m-%d"
            }

            interceptor = LoggingInterceptor(config)

            assert interceptor.log_level == logging.DEBUG
            assert interceptor.log_request is False
            assert interceptor.log_response is False
            assert interceptor.log_errors is False
            assert interceptor.provide_context_logger is False
            assert interceptor.destinations == ["file"]
            assert interceptor.file_path == log_path
            assert interceptor.log_format == "Custom format: %(message)s"
            assert interceptor.date_format == "%Y-%m-%d"

    def test_init_invalid_log_level(self):
        """Test initialization with invalid log level falls back to INFO."""
        config = {"log_level": "INVALID_LEVEL"}
        interceptor = LoggingInterceptor(config)
        assert interceptor.log_level == logging.INFO

    def test_create_handler_stdout(self):
        """Test creating stdout handler."""
        interceptor = LoggingInterceptor({"destinations": []})  # Don't setup handlers during init
        handler = interceptor._create_handler("stdout")

        assert handler is not None
        assert isinstance(handler, logging.StreamHandler)

    @patch('interceptors.logging.logging.FileHandler')
    @patch('interceptors.logging.os.makedirs')
    def test_create_handler_file(self, mock_makedirs, mock_file_handler):
        """Test creating file handler."""
        mock_handler = Mock()
        mock_file_handler.return_value = mock_handler

        interceptor = LoggingInterceptor({"file_path": "/test/logs/app.log"})
        handler = interceptor._create_handler("file")

        assert handler == mock_handler
        mock_makedirs.assert_called_once_with("/test/logs", exist_ok=True)
        mock_file_handler.assert_called_once_with("/test/logs/app.log", mode='a')

    def test_create_handler_unknown(self):
        """Test creating handler for unknown destination."""
        interceptor = LoggingInterceptor()
        handler = interceptor._create_handler("unknown_destination")

        assert handler is None

    def test_before_adds_context_logger(self):
        """Test that before() adds context logger."""
        config = {"provide_context_logger": True}
        interceptor = LoggingInterceptor(config)

        context = {"service_id": "test_service", "request_id": "req_123"}

        result_context = interceptor.before(context)

        assert "_logger" in result_context
        assert isinstance(result_context["_logger"], ContextLogger)
        assert "_logging_start_time" in result_context

    def test_before_logs_request(self):
        """Test that before() logs request when enabled."""
        config = {"log_request": True}
        interceptor = LoggingInterceptor(config)

        context = {"service_id": "test_service", "request_id": "req_123"}

        with patch.object(interceptor.logger, 'info') as mock_info:
            interceptor.before(context)
            mock_info.assert_called()

    def test_before_no_logging_when_disabled(self):
        """Test that before() doesn't log when disabled."""
        config = {"log_request": False, "provide_context_logger": False}
        interceptor = LoggingInterceptor(config)

        context = {"service_id": "test_service", "request_id": "req_123"}

        with patch.object(interceptor.logger, 'info') as mock_info:
            interceptor.before(context)
            mock_info.assert_not_called()

    def test_after_removes_context_logger(self):
        """Test that after() removes context logger."""
        interceptor = LoggingInterceptor({"provide_context_logger": True})

        context = {
            "service_id": "test_service",
            "request_id": "req_123",
            "_logger": Mock(),
            "_logging_start_time": 1000.0
        }
        result = {"status": "success"}

        returned_result = interceptor.after(context, result)

        assert "_logger" not in context
        assert "_logging_start_time" not in context
        assert returned_result == result

    def test_after_logs_completion(self):
        """Test that after() logs completion when enabled."""
        config = {"log_response": True}
        interceptor = LoggingInterceptor(config)

        context = {
            "service_id": "test_service",
            "request_id": "req_123",
            "_logging_start_time": 1000.0
        }
        result = {"status": "success"}

        with patch.object(interceptor.logger, 'info') as mock_info:
            with patch('interceptors.logging.time.time', return_value=1001.5):
                interceptor.after(context, result)
                mock_info.assert_called()

    def test_on_error_logs_error(self):
        """Test that on_error() logs errors when enabled."""
        config = {"log_errors": True}
        interceptor = LoggingInterceptor(config)

        context = {
            "service_id": "error_service",
            "request_id": "error_req",
            "_logging_start_time": 1000.0
        }
        error = Exception("Test error")

        with patch.object(interceptor.logger, 'error') as mock_error:
            result = interceptor.on_error(context, error)

            assert result is None  # Should re-raise
            mock_error.assert_called()

    def test_on_error_cleans_up_context(self):
        """Test that on_error() cleans up context."""
        interceptor = LoggingInterceptor({"log_errors": False})

        context = {
            "service_id": "error_service",
            "request_id": "error_req",
            "_logger": Mock(),
            "_logging_start_time": 1000.0
        }
        error = Exception("Test error")

        interceptor.on_error(context, error)

        assert "_logger" not in context
        assert "_logging_start_time" not in context

    def test_interceptor_inheritance(self):
        """Test that interceptor properly inherits from Interceptor."""
        from framework.interceptor import Interceptor

        interceptor = LoggingInterceptor()
        assert isinstance(interceptor, Interceptor)

    def test_integration_with_file_logging(self):
        """Integration test with actual file logging."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_file = os.path.join(temp_dir, "test.log")

            config = {
                "destinations": ["file"],
                "file_path": log_file,
                "log_level": "INFO"
            }

            interceptor = LoggingInterceptor(config)

            context = {"service_id": "file_test", "request_id": "file_req"}

            # Simulate before/after flow
            context = interceptor.before(context)
            result = {"status": "complete"}
            interceptor.after(context, result)

            # Verify file was created and contains logs
            assert os.path.exists(log_file)

            with open(log_file, 'r') as f:
                log_content = f.read()
                assert "file_test" in log_content
                assert "file_req" in log_content
