from typing import Any
from uuid import UUID

import pytest

from cascade.runtime.registry import (
    DuplicateTaskTypeError,
    TaskRegistry,
    TaskRegistryError,
    UnknownTaskTypeError,
)


def _handler(config: dict[str, Any], inputs: dict[UUID, Any]) -> Any:
    return config.get("value", 0)


def test_register_and_get_returns_the_handler() -> None:
    registry = TaskRegistry()

    registry.register("constant", _handler)

    assert registry.get("constant") is _handler


def test_duplicate_registration_is_rejected_and_preserves_first_handler() -> None:
    registry = TaskRegistry()
    registry.register("constant", _handler)

    def other_handler(config: dict[str, Any], inputs: dict[UUID, Any]) -> Any:
        return 1

    with pytest.raises(DuplicateTaskTypeError, match="already registered"):
        registry.register("constant", other_handler)

    assert registry.get("constant") is _handler


def test_unknown_type_raises_explicit_error() -> None:
    registry = TaskRegistry()

    with pytest.raises(UnknownTaskTypeError, match="no handler registered"):
        registry.get("missing")


def test_registry_errors_are_value_errors() -> None:
    assert issubclass(TaskRegistryError, ValueError)
    assert issubclass(DuplicateTaskTypeError, TaskRegistryError)
    assert issubclass(UnknownTaskTypeError, TaskRegistryError)


def test_registries_do_not_share_state() -> None:
    first = TaskRegistry()
    second = TaskRegistry()

    first.register("constant", _handler)

    with pytest.raises(UnknownTaskTypeError):
        second.get("constant")
