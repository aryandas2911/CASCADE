"""Sequential local DAG executor.

This is deliberately a synchronous, in-process foundation. It does not provide
asynchronous execution, retries, timeouts, cancellation, or distributed state.
"""

from uuid import UUID

from cascade.runtime.models import (
    NodeExecutionResult,
    TaskStatus,
    WorkflowExecutionResult,
    WorkflowStatus,
)
from cascade.runtime.registry import TaskRegistry, UnknownTaskTypeError
from cascade.workflow.graph import topological_sort, validate_dag
from cascade.workflow.schemas import Workflow


class LocalExecutor:
    """Execute a validated workflow DAG sequentially in topological order."""

    def __init__(self, registry: TaskRegistry) -> None:
        self._registry = registry

    def execute(self, workflow: Workflow) -> WorkflowExecutionResult:
        """Execute every node, stopping at the first failure.

        The full DAG is validated before any handler runs. On failure, the
        returned result preserves successful outputs, marks the failing node
        ``FAILED``, and leaves unstarted nodes ``PENDING``.

        Raises:
            DAGCycleError: If the workflow DAG contains a cycle.
        """
        dag = workflow.dag
        validate_dag(dag)
        order = topological_sort(dag)

        node_by_id = {node.id: node for node in dag.nodes}
        predecessors: dict[UUID, list[UUID]] = {node.id: [] for node in dag.nodes}
        for edge in dag.edges:
            predecessors[edge.to_node].append(edge.from_node)

        result = WorkflowExecutionResult(
            workflow_id=workflow.id,
            status=WorkflowStatus.RUNNING,
            node_results={
                node.id: NodeExecutionResult(node_id=node.id) for node in dag.nodes
            },
        )

        for node_id in order:
            node = node_by_id[node_id]
            node_result = result.node_results[node_id]

            try:
                handler = self._registry.get(node.type)
            except UnknownTaskTypeError as error:
                node_result.status = TaskStatus.FAILED
                node_result.error = str(error)
                result.status = WorkflowStatus.FAILED
                return result

            inputs = {
                parent_id: result.node_results[parent_id].output
                for parent_id in predecessors[node_id]
            }

            node_result.status = TaskStatus.RUNNING
            try:
                output = handler(node.config, inputs)
            except Exception as error:  # Failures are recorded, not swallowed.
                node_result.status = TaskStatus.FAILED
                node_result.error = f"{type(error).__name__}: {error}"
                result.status = WorkflowStatus.FAILED
                return result

            node_result.output = output
            node_result.status = TaskStatus.SUCCEEDED

        result.status = WorkflowStatus.COMPLETED
        return result
