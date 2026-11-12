"""Handler registry for local workflow execution."""

from collections.abc import Callable
from typing import Any
from uuid import UUID

TaskHandler = Callable[[dict[str, Any], dict[UUID, Any]], Any]


class TaskRegistryError(ValueError):
    """Base error for task registry failures."""


class DuplicateTaskTypeError(TaskRegistryError):
    """Raised when a node type is registered more than once."""


class UnknownTaskTypeError(TaskRegistryError):
    """Raised when no handler is registered for a node type."""


class TaskRegistry:
    """A per-executor mapping from node type to synchronous handler."""

    def __init__(self) -> None:
        self._handlers: dict[str, TaskHandler] = {}

    def register(self, node_type: str, handler: TaskHandler) -> None:
        """Register a handler for ``node_type``.

        Raises:
            DuplicateTaskTypeError: If the type already has a handler.
        """
        if node_type in self._handlers:
            raise DuplicateTaskTypeError(
                f"handler already registered for node type '{node_type}'"
            )
        self._handlers[node_type] = handler

    def get(self, node_type: str) -> TaskHandler:
        """Return the handler for ``node_type``.

        Raises:
            UnknownTaskTypeError: If the type has no registered handler.
        """
        try:
            return self._handlers[node_type]
        except KeyError:
            raise UnknownTaskTypeError(
                f"no handler registered for node type '{node_type}'"
            ) from None
