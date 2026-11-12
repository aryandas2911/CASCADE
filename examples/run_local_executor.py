"""Run a three-node workflow with the sequential local executor.

From the repository root:

    python examples/run_local_executor.py

The workflow computes ``((constant * factor) + offset)``.
"""

from typing import Any
from uuid import UUID

from cascade.runtime import LocalExecutor, TaskRegistry, WorkflowStatus
from cascade.workflow import DAG, Edge, Node, Workflow

WORKFLOW_ID = UUID("82000000-0000-4000-8000-000000000001")
CONSTANT_ID = UUID("82100000-0000-4000-8000-000000000001")
MULTIPLY_ID = UUID("82100000-0000-4000-8000-000000000002")
COMBINE_ID = UUID("82100000-0000-4000-8000-000000000003")


def constant(config: dict[str, Any], inputs: dict[UUID, Any]) -> Any:
    return config["value"]


def multiply(config: dict[str, Any], inputs: dict[UUID, Any]) -> Any:
    (value,) = inputs.values()
    return value * config["factor"]


def combine(config: dict[str, Any], inputs: dict[UUID, Any]) -> Any:
    (value,) = inputs.values()
    return value + config["offset"]


def build_workflow() -> Workflow:
    return Workflow(
        id=WORKFLOW_ID,
        name="Sequential Executor Example",
        dag=DAG(
            nodes=[
                Node(id=CONSTANT_ID, type="constant", config={"value": 6}),
                Node(id=MULTIPLY_ID, type="multiply", config={"factor": 7}),
                Node(id=COMBINE_ID, type="combine", config={"offset": 8}),
            ],
            edges=[
                Edge(from_node=CONSTANT_ID, to_node=MULTIPLY_ID),
                Edge(from_node=MULTIPLY_ID, to_node=COMBINE_ID),
            ],
        ),
    )


def main() -> None:
    registry = TaskRegistry()
    registry.register("constant", constant)
    registry.register("multiply", multiply)
    registry.register("combine", combine)

    result = LocalExecutor(registry).execute(build_workflow())

    print(f"Workflow status: {result.status.value}")
    for node_id, node_result in result.node_results.items():
        print(
            f"{node_id} {node_result.status.value} "
            f"output={node_result.output} error={node_result.error}"
        )

    if result.status is not WorkflowStatus.COMPLETED:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
