from unittest.mock import Mock

from framework.interceptor_registry import InterceptorRegistry


def _register(registry: InterceptorRegistry, name: str, order: int, scope: dict | None = None, enabled: bool = True) -> None:
    config = {
        "module": "test.module",
        "class": "TestInterceptor",
        "order": order,
        "enabled": enabled
    }
    if scope is not None:
        config["scope"] = scope
    registry.register_interceptor(name, config)


def test_global_interceptor_applies_to_all() -> None:
    registry = InterceptorRegistry()
    _register(registry, "global", 10)
    mock_interceptor = Mock()
    registry._interceptor_cache["global"] = mock_interceptor

    interceptors = registry.get_enabled_interceptors_for_service("service-a")
    assert interceptors == [mock_interceptor]


def test_include_services_limits() -> None:
    registry = InterceptorRegistry()
    _register(registry, "limited", 10, scope={"include_services": ["service-a"]})
    mock_interceptor = Mock()
    registry._interceptor_cache["limited"] = mock_interceptor

    assert registry.get_enabled_interceptors_for_service("service-a") == [mock_interceptor]
    assert registry.get_enabled_interceptors_for_service("service-b") == []


def test_exclude_services_filters() -> None:
    registry = InterceptorRegistry()
    _register(registry, "excluded", 10, scope={"exclude_services": ["service-b"]})
    mock_interceptor = Mock()
    registry._interceptor_cache["excluded"] = mock_interceptor

    assert registry.get_enabled_interceptors_for_service("service-a") == [mock_interceptor]
    assert registry.get_enabled_interceptors_for_service("service-b") == []


def test_include_and_exclude_combination() -> None:
    registry = InterceptorRegistry()
    _register(
        registry,
        "combined",
        10,
        scope={"include_services": ["service-a", "service-b"], "exclude_services": ["service-b"]}
    )
    mock_interceptor = Mock()
    registry._interceptor_cache["combined"] = mock_interceptor

    assert registry.get_enabled_interceptors_for_service("service-a") == [mock_interceptor]
    assert registry.get_enabled_interceptors_for_service("service-b") == []


def test_disabled_interceptors_are_ignored() -> None:
    registry = InterceptorRegistry()
    _register(registry, "disabled", 10, enabled=False)

    assert registry.get_enabled_interceptors_for_service("service-a") == []


def test_ordering_respected_after_filtering() -> None:
    registry = InterceptorRegistry()
    _register(registry, "late", 30)
    _register(registry, "early", 10, scope={"include_services": ["service-a"]})
    _register(registry, "middle", 20, scope={"include_services": ["service-a"]})

    mock_late = Mock()
    mock_early = Mock()
    mock_middle = Mock()
    registry._interceptor_cache["late"] = mock_late
    registry._interceptor_cache["early"] = mock_early
    registry._interceptor_cache["middle"] = mock_middle

    interceptors = registry.get_enabled_interceptors_for_service("service-a")
    assert interceptors == [mock_early, mock_middle, mock_late]
