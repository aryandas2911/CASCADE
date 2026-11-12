"""Cascade runtime package."""

from cascade.runtime.executor import LocalExecutor
from cascade.runtime.models import (
    NodeExecutionResult,
    TaskStatus,
    WorkflowExecutionResult,
    WorkflowStatus,
)
from cascade.runtime.registry import (
    DuplicateTaskTypeError,
    TaskHandler,
    TaskRegistry,
    TaskRegistryError,
    UnknownTaskTypeError,
)


def initialize_runtime() -> None:
    """Initialize the Cascade runtime."""
    print("Cascade runtime initialized")


__all__ = [
    "DuplicateTaskTypeError",
    "LocalExecutor",
    "NodeExecutionResult",
    "TaskHandler",
    "TaskRegistry",
    "TaskRegistryError",
    "TaskStatus",
    "UnknownTaskTypeError",
    "WorkflowExecutionResult",
    "WorkflowStatus",
    "initialize_runtime",
]
