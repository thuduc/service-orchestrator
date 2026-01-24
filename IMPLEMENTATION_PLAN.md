# Implementation Plan: Service-Scoped Interceptors

## Goals
- Allow interceptors to be scoped to specific services or excluded from specific services.
- Preserve existing global interceptor behavior by default.
- Validate interceptor scope configuration against known services.
- Provide comprehensive unit test coverage for configuration validation and runtime filtering.

## Design Overview
- Extend `interceptors.json` to support a `scope` object on each interceptor.
- Use `scope.include_services` and `scope.exclude_services` to filter interceptors per service.
- Update interceptor registry to expose a per-service lookup method.
- Build interceptor pipeline per request using the service ID.
- Expand config validation to enforce scope rules and validate service IDs.

## Configuration Changes
- `interceptors.json` schema:
  - `scope` (optional object)
    - `include_services` (optional array of strings)
    - `exclude_services` (optional array of strings)
- Scope rules:
  - Missing `scope` means global.
  - `include_services` limits the interceptor to listed services.
  - `exclude_services` removes listed services from otherwise eligible set.
  - Both present means include first, then exclude.

## Implementation Details

### 1) Config validation updates
- File: `framework/config_validator.py`
- Add `self.known_services: Set[str]` in `ConfigValidator.__init__`.
- Populate `self.known_services` in `_validate_services_config` from `services.json`.
- Extend `_validate_interceptor` to:
  - Validate `scope` type (object).
  - Validate `scope.include_services` and `scope.exclude_services` as arrays of strings.
  - Warn on duplicates in include/exclude lists.
  - Error on unknown service IDs if `known_services` is populated.
  - Warn if exclude removes all included services.

### 2) Interceptor registry updates
- File: `framework/interceptor_registry.py`
- Store `scope` in the registry entry (default empty dict).
- Add `get_enabled_interceptors_for_service(service_id: str) -> List[Interceptor]`:
  - Filter enabled interceptors by scope rules.
  - Sort by `order` ascending.
  - Instantiate interceptors via existing `get_interceptor`.
- Preserve existing `get_enabled_interceptors` for backward compatibility.

### 3) Service entrypoint updates
- File: `framework/service_entrypoint.py`
- Build interceptor pipeline per request using `service_id`:
  - When `interceptor_pipeline` is not explicitly provided, assemble pipeline in `execute`.
  - Use `InterceptorRegistry.get_enabled_interceptors_for_service(service_id)` to populate pipeline.
  - Cache pipelines by `service_id` to avoid rebuilding on every request.
- Maintain current behavior when a pipeline is passed in explicitly.

### 4) Documentation updates
- Update `FRAMEWORK_DESIGN.md` and/or `README.md` with:
  - New `scope` fields and examples.
  - Description of per-service interceptor behavior.

## Unit Testing Plan

### Test coverage goals
- Validate scope configuration errors and warnings.
- Validate interceptor filtering logic against include/exclude rules.
- Ensure backward compatibility for global interceptors.
- Verify entrypoint builds per-service pipeline and applies correct interceptors.

### Test cases

#### Config validator tests
- `scope` missing -> no errors.
- `scope` not object -> error.
- `include_services` not list of strings -> error.
- `exclude_services` not list of strings -> error.
- Duplicate entries in include/exclude -> warning.
- Unknown service IDs in include/exclude -> error (when `known_services` present).
- include + exclude removes all included -> warning.

#### Registry filtering tests
- Global interceptor with no scope applies to all services.
- `include_services` limits to listed services.
- `exclude_services` removes listed services.
- Both include + exclude: included minus excluded.
- Disabled interceptors are ignored.
- Ordering respects `order` field after filtering.

#### Entrypoint pipeline tests
- For a given `service_id`, pipeline includes only matching interceptors.
- For a different `service_id`, pipeline differs accordingly.
- If a pipeline is passed explicitly, service-specific filtering is not applied.

## Proposed Test Structure
- New tests in `tests/`:
  - `tests/test_config_validator_interceptor_scope.py`
  - `tests/test_interceptor_registry_scope.py`
  - `tests/test_service_entrypoint_scoped_interceptors.py`
- Use small fixture configs for `services.json` and `interceptors.json` with temp files.
- Mock or stub interceptor classes to avoid real side effects.

## Rollout Plan
1) Implement config schema + validator updates.
2) Implement registry filtering logic.
3) Update service entrypoint pipeline behavior.
4) Add tests and confirm pass.
5) Update docs with examples.

## Risks and Mitigations
- Risk: misconfigured scopes cause silent exclusions. Mitigation: validator errors + warnings.
- Risk: per-request pipeline build overhead. Mitigation: cache pipelines by `service_id` and reuse registry interceptor instances.
- Risk: ambiguity when both include/exclude are empty arrays. Mitigation: treat empty arrays as unset.
