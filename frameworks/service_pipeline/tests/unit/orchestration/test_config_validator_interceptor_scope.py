import json
from pathlib import Path

from frameworks.service_pipeline.orchestration.config_validator import ConfigValidator


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload))


def _services_config() -> dict:
    return {
        "services": {
            "service-a": {"steps": [{"module": "frameworks.service_pipeline.implementation.components.pre_calibration", "class": "PreCalibrationComponent"}]},
            "service-b": {"steps": [{"module": "frameworks.service_pipeline.implementation.components.simulation", "class": "SimulationComponent"}]}
        }
    }


def test_scope_missing_is_valid(tmp_path: Path) -> None:
    services_path = tmp_path / "services.json"
    interceptors_path = tmp_path / "interceptors.json"

    _write_json(services_path, _services_config())
    _write_json(
        interceptors_path,
        {
            "interceptors": {
                "logging": {"module": "frameworks.service_pipeline.implementation.interceptors.logging", "class": "LoggingInterceptor"}
            }
        },
    )

    validator = ConfigValidator(str(services_path), str(interceptors_path))
    assert validator.validate() is True
    assert validator.errors == []


def test_scope_invalid_type(tmp_path: Path) -> None:
    services_path = tmp_path / "services.json"
    interceptors_path = tmp_path / "interceptors.json"

    _write_json(services_path, _services_config())
    _write_json(
        interceptors_path,
        {
            "interceptors": {
                "validation": {
                    "module": "frameworks.service_pipeline.implementation.interceptors.validation",
                    "class": "ValidationInterceptor",
                    "scope": "all"
                }
            }
        },
    )

    validator = ConfigValidator(str(services_path), str(interceptors_path))
    assert validator.validate() is False
    assert "'scope' must be an object" in " ".join(validator.errors)


def test_scope_include_requires_array_of_strings(tmp_path: Path) -> None:
    services_path = tmp_path / "services.json"
    interceptors_path = tmp_path / "interceptors.json"

    _write_json(services_path, _services_config())
    _write_json(
        interceptors_path,
        {
            "interceptors": {
                "validation": {
                    "module": "frameworks.service_pipeline.implementation.interceptors.validation",
                    "class": "ValidationInterceptor",
                    "scope": {"include_services": "service-a"}
                }
            }
        },
    )

    validator = ConfigValidator(str(services_path), str(interceptors_path))
    assert validator.validate() is False
    assert "'scope.include_services' must be an array of strings" in " ".join(validator.errors)


def test_scope_exclude_requires_array_of_strings(tmp_path: Path) -> None:
    services_path = tmp_path / "services.json"
    interceptors_path = tmp_path / "interceptors.json"

    _write_json(services_path, _services_config())
    _write_json(
        interceptors_path,
        {
            "interceptors": {
                "validation": {
                    "module": "frameworks.service_pipeline.implementation.interceptors.validation",
                    "class": "ValidationInterceptor",
                    "scope": {"exclude_services": ["service-a", 123]}
                }
            }
        },
    )

    validator = ConfigValidator(str(services_path), str(interceptors_path))
    assert validator.validate() is False
    assert "'scope.exclude_services' must be an array of strings" in " ".join(validator.errors)


def test_scope_duplicate_entries_warn(tmp_path: Path) -> None:
    services_path = tmp_path / "services.json"
    interceptors_path = tmp_path / "interceptors.json"

    _write_json(services_path, _services_config())
    _write_json(
        interceptors_path,
        {
            "interceptors": {
                "validation": {
                    "module": "frameworks.service_pipeline.implementation.interceptors.validation",
                    "class": "ValidationInterceptor",
                    "scope": {
                        "include_services": ["service-a", "service-a"],
                        "exclude_services": ["service-b", "service-b"]
                    }
                }
            }
        },
    )

    validator = ConfigValidator(str(services_path), str(interceptors_path))
    assert validator.validate() is True
    warning_text = " ".join(validator.warnings)
    assert "'scope.include_services' contains duplicates" in warning_text
    assert "'scope.exclude_services' contains duplicates" in warning_text


def test_scope_unknown_service_ids_error(tmp_path: Path) -> None:
    services_path = tmp_path / "services.json"
    interceptors_path = tmp_path / "interceptors.json"

    _write_json(services_path, _services_config())
    _write_json(
        interceptors_path,
        {
            "interceptors": {
                "validation": {
                    "module": "frameworks.service_pipeline.implementation.interceptors.validation",
                    "class": "ValidationInterceptor",
                    "scope": {
                        "include_services": ["service-a", "missing"],
                        "exclude_services": ["unknown"]
                    }
                }
            }
        },
    )

    validator = ConfigValidator(str(services_path), str(interceptors_path))
    assert validator.validate() is False
    errors = " ".join(validator.errors)
    assert "Unknown service_id in 'scope.include_services': 'missing'" in errors
    assert "Unknown service_id in 'scope.exclude_services': 'unknown'" in errors


def test_scope_exclude_removes_all_included_warn(tmp_path: Path) -> None:
    services_path = tmp_path / "services.json"
    interceptors_path = tmp_path / "interceptors.json"

    _write_json(services_path, _services_config())
    _write_json(
        interceptors_path,
        {
            "interceptors": {
                "validation": {
                    "module": "frameworks.service_pipeline.implementation.interceptors.validation",
                    "class": "ValidationInterceptor",
                    "scope": {
                        "include_services": ["service-a"],
                        "exclude_services": ["service-a"]
                    }
                }
            }
        },
    )

    validator = ConfigValidator(str(services_path), str(interceptors_path))
    assert validator.validate() is True
    assert "'scope.exclude_services' removes all included services" in " ".join(validator.warnings)
