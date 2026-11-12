from itertools import permutations
from typing import Any
from uuid import UUID

import pytest

from cascade.runtime import (
    LocalExecutor,
    TaskHandler,
    TaskRegistry,
    TaskStatus,
    WorkflowStatus,
)
from cascade.workflow import DAG, DAGCycleError, Edge, Node, Workflow

WORKFLOW_ID = UUID("80000000-0000-4000-8000-000000000001")

NODE_A = UUID("81000000-0000-4000-8000-000000000001")
NODE_B = UUID("81000000-0000-4000-8000-000000000002")
NODE_C = UUID("81000000-0000-4000-8000-000000000003")
NODE_D = UUID("81000000-0000-4000-8000-000000000004")


def _node(
    node_id: UUID,
    node_type: str,
    config: dict[str, Any] | None = None,
) -> Node:
    return Node(id=node_id, type=node_type, config=config or {})


def _workflow(
    nodes: list[Node],
    edges: list[tuple[UUID, UUID]],
) -> Workflow:
    return Workflow(
        id=WORKFLOW_ID,
        name="Executor Test",
        dag=DAG(
            nodes=nodes,
            edges=[
                Edge(from_node=from_node, to_node=to_node)
                for from_node, to_node in edges
            ],
        ),
    )


def _recording_handler(order: list[str]) -> TaskHandler:
    def record(config: dict[str, Any], inputs: dict[UUID, Any]) -> Any:
        order.append(config["label"])
        return config["label"]

    return record


def test_single_node_executes_with_empty_inputs() -> None:
    seen: list[tuple[dict[str, Any], dict[UUID, Any]]] = []

    def double(config: dict[str, Any], inputs: dict[UUID, Any]) -> Any:
        seen.append((config, inputs))
        return config["value"] * 2

    registry = TaskRegistry()
    registry.register("double", double)
    workflow = _workflow([_node(NODE_A, "double", {"value": 5})], [])

    result = LocalExecutor(registry).execute(workflow)

    node_result = result.node_results[NODE_A]
    assert result.workflow_id == WORKFLOW_ID
    assert result.status is WorkflowStatus.COMPLETED
    assert node_result.status is TaskStatus.SUCCEEDED
    assert node_result.output == 10
    assert node_result.error is None
    assert seen == [({"value": 5}, {})]


def test_linear_chain_passes_upstream_outputs() -> None:
    received: dict[UUID, dict[UUID, Any]] = {}

    def constant(config: dict[str, Any], inputs: dict[UUID, Any]) -> Any:
        return config["value"]

    def multiply(config: dict[str, Any], inputs: dict[UUID, Any]) -> Any:
        received[NODE_B] = dict(inputs)
        return next(iter(inputs.values())) * config["factor"]

    def combine(config: dict[str, Any], inputs: dict[UUID, Any]) -> Any:
        received[NODE_C] = dict(inputs)
        return next(iter(inputs.values())) + config["offset"]

    registry = TaskRegistry()
    registry.register("constant", constant)
    registry.register("multiply", multiply)
    registry.register("combine", combine)
    workflow = _workflow(
        [
            _node(NODE_A, "constant", {"value": 2}),
            _node(NODE_B, "multiply", {"factor": 3}),
            _node(NODE_C, "combine", {"offset": 4}),
        ],
        [(NODE_A, NODE_B), (NODE_B, NODE_C)],
    )

    result = LocalExecutor(registry).execute(workflow)

    assert result.status is WorkflowStatus.COMPLETED
    statuses = [
        result.node_results[node_id].status
        for node_id in (NODE_A, NODE_B, NODE_C)
    ]
    assert statuses == [TaskStatus.SUCCEEDED] * 3
    assert result.node_results[NODE_A].output == 2
    assert result.node_results[NODE_B].output == 6
    assert result.node_results[NODE_C].output == 10
    assert received[NODE_B] == {NODE_A: 2}
    assert received[NODE_C] == {NODE_B: 6}


def test_diamond_receives_only_direct_predecessor_outputs() -> None:
    received: dict[UUID, dict[UUID, Any]] = {}

    def source(config: dict[str, Any], inputs: dict[UUID, Any]) -> Any:
        return 1

    def left(config: dict[str, Any], inputs: dict[UUID, Any]) -> Any:
        received[NODE_B] = dict(inputs)
        return next(iter(inputs.values())) + 10

    def right(config: dict[str, Any], inputs: dict[UUID, Any]) -> Any:
        received[NODE_C] = dict(inputs)
        return next(iter(inputs.values())) + 20

    def join(config: dict[str, Any], inputs: dict[UUID, Any]) -> Any:
        received[NODE_D] = dict(inputs)
        return sum(inputs.values())

    registry = TaskRegistry()
    registry.register("source", source)
    registry.register("left", left)
    registry.register("right", right)
    registry.register("join", join)
    workflow = _workflow(
        [
            _node(NODE_A, "source"),
            _node(NODE_B, "left"),
            _node(NODE_C, "right"),
            _node(NODE_D, "join"),
        ],
        [
            (NODE_A, NODE_B),
            (NODE_A, NODE_C),
            (NODE_B, NODE_D),
            (NODE_C, NODE_D),
        ],
    )

    result = LocalExecutor(registry).execute(workflow)

    assert result.status is WorkflowStatus.COMPLETED
    assert result.node_results[NODE_B].output == 11
    assert result.node_results[NODE_C].output == 21
    assert result.node_results[NODE_D].output == 32
    assert received[NODE_B] == {NODE_A: 1}
    assert received[NODE_C] == {NODE_A: 1}
    assert received[NODE_D] == {NODE_B: 11, NODE_C: 21}
    assert NODE_A not in received[NODE_D]


def test_disconnected_nodes_execute_in_deterministic_order() -> None:
    order: list[str] = []
    received: dict[str, dict[UUID, Any]] = {}

    def record(config: dict[str, Any], inputs: dict[UUID, Any]) -> Any:
        order.append(config["label"])
        received[config["label"]] = dict(inputs)
        return config["label"]

    registry = TaskRegistry()
    registry.register("record", record)
    workflow = _workflow(
        [
            _node(NODE_A, "record", {"label": "A"}),
            _node(NODE_B, "record", {"label": "B"}),
            _node(NODE_C, "record", {"label": "C"}),
        ],
        [(NODE_A, NODE_B)],
    )

    result = LocalExecutor(registry).execute(workflow)

    # A and C are ready first; B becomes ready after A and has a smaller UUID.
    assert order == ["A", "B", "C"]
    assert received["B"] == {NODE_A: "A"}
    assert received["C"] == {}
    assert result.status is WorkflowStatus.COMPLETED


def test_execution_order_is_deterministic_under_shuffled_insertion() -> None:
    nodes = [
        _node(NODE_A, "record", {"label": "A"}),
        _node(NODE_B, "record", {"label": "B"}),
        _node(NODE_C, "record", {"label": "C"}),
        _node(NODE_D, "record", {"label": "D"}),
    ]
    edges = [
        (NODE_A, NODE_B),
        (NODE_A, NODE_C),
        (NODE_B, NODE_D),
        (NODE_C, NODE_D),
    ]
    edge_orders = [edges, list(reversed(edges))]

    for node_order in permutations(nodes):
        for edge_order in edge_orders:
            order: list[str] = []
            registry = TaskRegistry()
            registry.register("record", _recording_handler(order))
            workflow = _workflow(list(node_order), edge_order)

            result = LocalExecutor(registry).execute(workflow)

            assert result.status is WorkflowStatus.COMPLETED
            assert order == ["A", "B", "C", "D"]


def test_handler_exception_stops_execution_and_preserves_partial_result() -> None:
    calls: list[UUID] = []

    def ok(config: dict[str, Any], inputs: dict[UUID, Any]) -> Any:
        calls.append(NODE_A)
        return 7

    def explode(config: dict[str, Any], inputs: dict[UUID, Any]) -> Any:
        calls.append(NODE_B)
        raise ValueError("boom")

    def never(config: dict[str, Any], inputs: dict[UUID, Any]) -> Any:
        calls.append(NODE_C)
        return 0

    registry = TaskRegistry()
    registry.register("ok", ok)
    registry.register("explode", explode)
    registry.register("never", never)
    workflow = _workflow(
        [
            _node(NODE_A, "ok"),
            _node(NODE_B, "explode"),
            _node(NODE_C, "never"),
        ],
        [(NODE_A, NODE_B), (NODE_B, NODE_C)],
    )

    result = LocalExecutor(registry).execute(workflow)

    assert calls == [NODE_A, NODE_B]
    assert result.status is WorkflowStatus.FAILED

    first = result.node_results[NODE_A]
    assert first.status is TaskStatus.SUCCEEDED
    assert first.output == 7
    assert first.error is None

    failed = result.node_results[NODE_B]
    assert failed.status is TaskStatus.FAILED
    assert failed.output is None
    assert failed.error is not None
    assert "ValueError" in failed.error
    assert "boom" in failed.error

    never_started = result.node_results[NODE_C]
    assert never_started.status is TaskStatus.PENDING
    assert never_started.output is None
    assert never_started.error is None


def test_missing_handler_fails_node_and_leaves_downstream_pending() -> None:
    calls: list[UUID] = []

    def known(config: dict[str, Any], inputs: dict[UUID, Any]) -> Any:
        calls.append(config["node_id"])
        return "ok"

    registry = TaskRegistry()
    registry.register("known", known)
    workflow = _workflow(
        [
            _node(NODE_A, "known", {"node_id": NODE_A}),
            _node(NODE_B, "missing"),
            _node(NODE_C, "known", {"node_id": NODE_C}),
        ],
        [(NODE_A, NODE_B), (NODE_B, NODE_C)],
    )

    result = LocalExecutor(registry).execute(workflow)

    assert calls == [NODE_A]
    assert result.status is WorkflowStatus.FAILED
    assert result.node_results[NODE_A].status is TaskStatus.SUCCEEDED

    failed = result.node_results[NODE_B]
    assert failed.status is TaskStatus.FAILED
    assert failed.error is not None
    assert "missing" in failed.error
    assert "no handler registered" in failed.error

    assert result.node_results[NODE_C].status is TaskStatus.PENDING


def test_cycle_is_rejected_before_any_handler_runs() -> None:
    calls: list[UUID] = []

    def handler(config: dict[str, Any], inputs: dict[UUID, Any]) -> Any:
        calls.append(NODE_A)
        return 0

    registry = TaskRegistry()
    registry.register("task", handler)
    workflow = _workflow(
        [_node(NODE_A, "task"), _node(NODE_B, "task")],
        [(NODE_A, NODE_B), (NODE_B, NODE_A)],
    )

    with pytest.raises(DAGCycleError):
        LocalExecutor(registry).execute(workflow)

    assert calls == []


def test_empty_dag_completes_successfully() -> None:
    result = LocalExecutor(TaskRegistry()).execute(_workflow([], []))

    assert result.workflow_id == WORKFLOW_ID
    assert result.status is WorkflowStatus.COMPLETED
    assert result.node_results == {}


def test_outputs_may_be_arbitrary_python_objects() -> None:
    sentinel = object()

    def handler(config: dict[str, Any], inputs: dict[UUID, Any]) -> Any:
        return sentinel

    registry = TaskRegistry()
    registry.register("object", handler)
    workflow = _workflow([_node(NODE_A, "object")], [])

    result = LocalExecutor(registry).execute(workflow)

    assert result.node_results[NODE_A].output is sentinel


def test_repeated_execution_does_not_reuse_result_objects() -> None:
    def constant(config: dict[str, Any], inputs: dict[UUID, Any]) -> Any:
        return 2

    def multiply(config: dict[str, Any], inputs: dict[UUID, Any]) -> Any:
        return next(iter(inputs.values())) * 3

    registry = TaskRegistry()
    registry.register("constant", constant)
    registry.register("multiply", multiply)
    workflow = _workflow(
        [_node(NODE_A, "constant"), _node(NODE_B, "multiply")],
        [(NODE_A, NODE_B)],
    )
    executor = LocalExecutor(registry)

    first = executor.execute(workflow)
    first.node_results[NODE_A].output = "mutated"
    first.status = WorkflowStatus.FAILED

    second = executor.execute(workflow)

    assert first is not second
    assert first.node_results is not second.node_results
    assert second.status is WorkflowStatus.COMPLETED
    assert second.node_results[NODE_A].output == 2
    assert second.node_results[NODE_B].output == 6


def test_repeated_execution_recovers_after_a_failure() -> None:
    calls = 0

    def flaky(config: dict[str, Any], inputs: dict[UUID, Any]) -> Any:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("first call fails")
        return 42

    registry = TaskRegistry()
    registry.register("flaky", flaky)
    workflow = _workflow([_node(NODE_A, "flaky")], [])
    executor = LocalExecutor(registry)

    first = executor.execute(workflow)
    second = executor.execute(workflow)

    assert first.status is WorkflowStatus.FAILED
    assert first.node_results[NODE_A].status is TaskStatus.FAILED
    assert second.status is WorkflowStatus.COMPLETED
    assert second.node_results[NODE_A].status is TaskStatus.SUCCEEDED
    assert second.node_results[NODE_A].output == 42
