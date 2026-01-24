import pytest
import json
from unittest.mock import Mock, patch, mock_open
from framework.interceptor_registry import InterceptorRegistry


class TestInterceptorRegistry:

    def test_init_without_config(self):
        """Test initializing registry without configuration file."""
        registry = InterceptorRegistry()
        assert registry.config_path is None
        assert registry._registry == {}
        assert registry._interceptor_cache == {}
        assert registry.global_config == {}

    def test_init_with_config_file(self):
        """Test initializing registry with configuration file."""
        with patch('builtins.open', mock_open()) as mock_file:
            with patch('framework.interceptor_registry.json.load') as mock_json:
                mock_json.return_value = {
                    "global_config": {"debug": True},
                    "interceptors": {
                        "test_interceptor": {
                            "module": "test.module",
                            "class": "TestInterceptor"
                        }
                    }
                }

                registry = InterceptorRegistry("test_config.json")

                assert registry.config_path == "test_config.json"
                assert "test_interceptor" in registry._registry
                assert registry.global_config == {"debug": True}

    def test_load_configuration_file_not_found(self):
        """Test loading configuration when file doesn't exist."""
        with patch('builtins.open', side_effect=FileNotFoundError()):
            registry = InterceptorRegistry("nonexistent.json")
            # Should not raise exception, just log warning
            assert registry._registry == {}

    def test_load_configuration_invalid_json(self):
        """Test loading configuration with invalid JSON."""
        with patch('builtins.open', mock_open()) as mock_file:
            with patch('framework.interceptor_registry.json.load', side_effect=json.JSONDecodeError("Invalid", "", 0)):
                with pytest.raises(json.JSONDecodeError):
                    InterceptorRegistry("invalid.json")

    def test_register_interceptor_minimal_config(self):
        """Test registering interceptor with minimal configuration."""
        registry = InterceptorRegistry()
        config = {
            "module": "test.module",
            "class": "TestInterceptor"
        }

        registry.register_interceptor("test", config)

        assert "test" in registry._registry
        interceptor_info = registry._registry["test"]
        assert interceptor_info["module"] == "test.module"
        assert interceptor_info["class"] == "TestInterceptor"
        assert interceptor_info["enabled"] is True
        assert interceptor_info["order"] == 999
        assert interceptor_info["config"] == {}
        assert interceptor_info["scope"] == {}

    def test_register_interceptor_full_config(self):
        """Test registering interceptor with full configuration."""
        registry = InterceptorRegistry()
        config = {
            "module": "test.module",
            "class": "TestInterceptor",
            "enabled": False,
            "order": 10,
            "config": {"param": "value"}
        }

        registry.register_interceptor("test", config)

        interceptor_info = registry._registry["test"]
        assert interceptor_info["enabled"] is False
        assert interceptor_info["order"] == 10
        assert interceptor_info["config"] == {"param": "value"}
        assert interceptor_info["scope"] == {}

    def test_register_interceptor_missing_module(self):
        """Test registering interceptor without required module field."""
        registry = InterceptorRegistry()
        config = {"class": "TestInterceptor"}

        with pytest.raises(ValueError, match="missing required field: module"):
            registry.register_interceptor("test", config)

    def test_register_interceptor_missing_class(self):
        """Test registering interceptor without required class field."""
        registry = InterceptorRegistry()
        config = {"module": "test.module"}

        with pytest.raises(ValueError, match="missing required field: class"):
            registry.register_interceptor("test", config)

    @patch('framework.interceptor_registry.importlib.import_module')
    @patch('framework.interceptor_registry.isinstance')
    def test_get_interceptor_success(self, mock_isinstance, mock_import):
        """Test successful interceptor retrieval."""
        registry = InterceptorRegistry()

        # Setup mocks
        mock_module = Mock()
        mock_interceptor_class = Mock()
        mock_interceptor_instance = Mock()

        mock_import.return_value = mock_module
        mock_module.TestInterceptor = mock_interceptor_class
        mock_interceptor_class.return_value = mock_interceptor_instance
        mock_isinstance.return_value = True  # Mock isinstance to return True for Interceptor interface check

        # Register and get interceptor
        config = {"module": "test.module", "class": "TestInterceptor", "config": {"param": "value"}}
        registry.register_interceptor("test", config)

        interceptor = registry.get_interceptor("test")

        assert interceptor == mock_interceptor_instance
        assert "test" in registry._interceptor_cache
        mock_import.assert_called_once_with("test.module")
        mock_interceptor_class.assert_called_once_with({"param": "value"})

    def test_get_interceptor_not_found(self):
        """Test getting interceptor that doesn't exist."""
        registry = InterceptorRegistry()

        interceptor = registry.get_interceptor("nonexistent")
        assert interceptor is None

    def test_get_interceptor_disabled(self):
        """Test getting disabled interceptor."""
        registry = InterceptorRegistry()
        config = {"module": "test.module", "class": "TestInterceptor", "enabled": False}
        registry.register_interceptor("test", config)

        interceptor = registry.get_interceptor("test")
        assert interceptor is None

    def test_get_interceptor_cached(self):
        """Test getting cached interceptor instance."""
        registry = InterceptorRegistry()

        # Setup mocks
        mock_interceptor_instance = Mock()

        # Pre-populate cache
        config = {"module": "test.module", "class": "TestInterceptor"}
        registry.register_interceptor("test", config)
        registry._interceptor_cache["test"] = mock_interceptor_instance

        interceptor = registry.get_interceptor("test")

        assert interceptor == mock_interceptor_instance

    @patch('framework.interceptor_registry.importlib.import_module')
    def test_get_interceptor_import_error(self, mock_import):
        """Test getting interceptor with import error."""
        registry = InterceptorRegistry()
        mock_import.side_effect = ImportError("Module not found")

        config = {"module": "nonexistent.module", "class": "TestInterceptor"}
        registry.register_interceptor("test", config)

        interceptor = registry.get_interceptor("test")
        assert interceptor is None

    @patch('framework.interceptor_registry.importlib.import_module')
    def test_get_interceptor_attribute_error(self, mock_import):
        """Test getting interceptor with attribute error."""
        registry = InterceptorRegistry()
        mock_module = Mock()
        mock_import.return_value = mock_module
        del mock_module.NonexistentClass

        config = {"module": "test.module", "class": "NonexistentClass"}
        registry.register_interceptor("test", config)

        interceptor = registry.get_interceptor("test")
        assert interceptor is None

    @patch('framework.interceptor_registry.importlib.import_module')
    def test_get_interceptor_not_interceptor_interface(self, mock_import):
        """Test getting interceptor that doesn't implement Interceptor interface."""
        registry = InterceptorRegistry()

        mock_module = Mock()
        mock_class = Mock()
        mock_instance = Mock()

        mock_import.return_value = mock_module
        mock_module.NotInterceptor = mock_class
        mock_class.return_value = mock_instance

        with patch('builtins.isinstance', return_value=False):
            config = {"module": "test.module", "class": "NotInterceptor"}
            registry.register_interceptor("test", config)

            interceptor = registry.get_interceptor("test")
            assert interceptor is None

    @patch('framework.interceptor_registry.importlib.import_module')
    def test_get_enabled_interceptors_empty(self, mock_import):
        """Test getting enabled interceptors when none are registered."""
        registry = InterceptorRegistry()
        interceptors = registry.get_enabled_interceptors()
        assert interceptors == []

    def test_get_enabled_interceptors_ordered(self):
        """Test getting enabled interceptors in correct order."""
        registry = InterceptorRegistry()

        # Pre-populate cache with mock interceptors
        mock_interceptor1 = Mock()
        mock_interceptor2 = Mock()
        mock_interceptor3 = Mock()

        # Register interceptors with different orders
        registry.register_interceptor("interceptor1", {
            "module": "test.interceptor1", "class": "Interceptor1", "order": 30
        })
        registry.register_interceptor("interceptor2", {
            "module": "test.interceptor2", "class": "Interceptor2", "order": 10
        })
        registry.register_interceptor("interceptor3", {
            "module": "test.interceptor3", "class": "Interceptor3", "order": 20
        })

        # Pre-populate cache
        registry._interceptor_cache["interceptor1"] = mock_interceptor1
        registry._interceptor_cache["interceptor2"] = mock_interceptor2
        registry._interceptor_cache["interceptor3"] = mock_interceptor3

        interceptors = registry.get_enabled_interceptors()

        assert len(interceptors) == 3
        # Should be ordered by order value: 10, 20, 30
        assert interceptors[0] == mock_interceptor2  # order 10
        assert interceptors[1] == mock_interceptor3  # order 20
        assert interceptors[2] == mock_interceptor1  # order 30

    def test_get_enabled_interceptors_disabled_excluded(self):
        """Test that disabled interceptors are excluded from enabled list."""
        registry = InterceptorRegistry()

        mock_interceptor = Mock()

        registry.register_interceptor("enabled", {
            "module": "test.module", "class": "EnabledInterceptor", "enabled": True
        })
        registry.register_interceptor("disabled", {
            "module": "test.module", "class": "DisabledInterceptor", "enabled": False
        })

        # Pre-populate cache for enabled one
        registry._interceptor_cache["enabled"] = mock_interceptor

        interceptors = registry.get_enabled_interceptors()

        # Only enabled interceptor should be returned (disabled returns None from get_interceptor)
        assert len(interceptors) == 1
        assert interceptors[0] == mock_interceptor

    def test_list_interceptors(self):
        """Test listing all registered interceptors."""
        registry = InterceptorRegistry()

        registry.register_interceptor("interceptor1", {
            "module": "test.module1", "class": "Interceptor1", "enabled": True, "order": 10
        })
        registry.register_interceptor("interceptor2", {
            "module": "test.module2", "class": "Interceptor2", "enabled": False, "order": 20
        })

        # Add one to cache
        registry._interceptor_cache["interceptor1"] = Mock()

        interceptors = registry.list_interceptors()

        assert len(interceptors) == 2
        assert interceptors["interceptor1"]["enabled"] is True
        assert interceptors["interceptor1"]["cached"] is True
        assert interceptors["interceptor1"]["scope"] == {}
        assert interceptors["interceptor2"]["enabled"] is False
        assert interceptors["interceptor2"]["cached"] is False
        assert interceptors["interceptor2"]["scope"] == {}

    def test_enable_interceptor(self):
        """Test enabling a disabled interceptor."""
        registry = InterceptorRegistry()

        registry.register_interceptor("test", {
            "module": "test.module", "class": "TestInterceptor", "enabled": False
        })

        assert registry._registry["test"]["enabled"] is False

        registry.enable_interceptor("test")

        assert registry._registry["test"]["enabled"] is True

    def test_enable_interceptor_nonexistent(self):
        """Test enabling non-existent interceptor."""
        registry = InterceptorRegistry()

        # Should not raise exception
        registry.enable_interceptor("nonexistent")

    def test_disable_interceptor(self):
        """Test disabling an enabled interceptor."""
        registry = InterceptorRegistry()

        registry.register_interceptor("test", {
            "module": "test.module", "class": "TestInterceptor", "enabled": True
        })

        # Add to cache
        registry._interceptor_cache["test"] = Mock()

        registry.disable_interceptor("test")

        assert registry._registry["test"]["enabled"] is False
        assert "test" not in registry._interceptor_cache

    def test_disable_interceptor_nonexistent(self):
        """Test disabling non-existent interceptor."""
        registry = InterceptorRegistry()

        # Should not raise exception
        registry.disable_interceptor("nonexistent")
