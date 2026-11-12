"""In-memory execution result models for the local runtime."""

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any
from uuid import UUID


class TaskStatus(StrEnum):
    """Lifecycle status of a single node execution."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class WorkflowStatus(StrEnum):
    """Lifecycle status of a local workflow execution."""

    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass(slots=True)
class NodeExecutionResult:
    """Local in-memory result for one workflow node."""

    node_id: UUID
    status: TaskStatus = TaskStatus.PENDING
    output: Any = None
    error: str | None = None


@dataclass(slots=True)
class WorkflowExecutionResult:
    """Local in-memory result for a workflow execution."""

    workflow_id: UUID
    status: WorkflowStatus = WorkflowStatus.RUNNING
    node_results: dict[UUID, NodeExecutionResult] = field(default_factory=dict)
