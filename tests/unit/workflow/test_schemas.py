from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from cascade.workflow import DAG, Edge, Node, Workflow, WorkflowConstraints

EXAMPLES_DIR = Path(__file__).resolve().parents[3] / "examples" / "workflows"

NODE_A = UUID("40000000-0000-4000-8000-000000000001")
NODE_B = UUID("40000000-0000-4000-8000-000000000002")
NODE_C = UUID("40000000-0000-4000-8000-000000000003")
WORKFLOW_ID = UUID("50000000-0000-4000-8000-000000000001")


@pytest.mark.parametrize(
    ("filename", "expected_nodes", "expected_edges"),
    [
        ("linear.json", 3, 2),
        ("branching.json", 3, 2),
    ],
)
def test_valid_example_workflows(
    filename: str, expected_nodes: int, expected_edges: int
) -> None:
    workflow = Workflow.model_validate_json(
        (EXAMPLES_DIR / filename).read_text(encoding="utf-8")
    )

    assert workflow.name
    assert len(workflow.dag.nodes) == expected_nodes
    assert len(workflow.dag.edges) == expected_edges


def test_branching_example_has_one_upstream_and_two_downstream_nodes() -> None:
    workflow = Workflow.model_validate_json(
        (EXAMPLES_DIR / "branching.json").read_text(encoding="utf-8")
    )

    upstream_id = workflow.dag.nodes[0].id
    assert {edge.from_node for edge in workflow.dag.edges} == {upstream_id}
    assert len({edge.to_node for edge in workflow.dag.edges}) == 2


def test_duplicate_node_ids_are_rejected() -> None:
    with pytest.raises(ValidationError, match="duplicate node id"):
        DAG(
            nodes=[
                Node(id=NODE_A, type="task"),
                Node(id=NODE_A, type="task"),
            ]
        )


@pytest.mark.parametrize(
    "edge",
    [
        Edge(from_node=NODE_A, to_node=NODE_B),
        Edge(from_node=NODE_B, to_node=NODE_A),
    ],
)
def test_edge_endpoints_must_exist(edge: Edge) -> None:
    with pytest.raises(ValidationError, match="unknown node id"):
        DAG(nodes=[Node(id=NODE_A, type="task")], edges=[edge])


def test_self_referencing_edge_is_rejected() -> None:
    with pytest.raises(ValidationError, match="self-referencing edge"):
        DAG(
            nodes=[Node(id=NODE_A, type="task")],
            edges=[Edge(from_node=NODE_A, to_node=NODE_A)],
        )


def test_duplicate_edges_are_rejected() -> None:
    edge = Edge(from_node=NODE_A, to_node=NODE_B)

    with pytest.raises(ValidationError, match="duplicate edge"):
        DAG(
            nodes=[Node(id=NODE_A, type="task"), Node(id=NODE_B, type="task")],
            edges=[edge, edge],
        )


@pytest.mark.parametrize(
    "node_data",
    [
        {"id": "not-a-uuid", "type": "task"},
        {"id": NODE_A, "type": ""},
        {"id": NODE_A, "type": "task", "config": "not-a-dict"},
        {"id": NODE_A, "type": "task", "estimated_cost": "expensive"},
    ],
)
def test_malformed_node_fields_are_rejected(node_data: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        Node(**node_data)


def test_empty_workflow_name_is_rejected() -> None:
    with pytest.raises(ValidationError):
        Workflow(id=WORKFLOW_ID, name="", dag=DAG())


def test_malformed_json_is_rejected() -> None:
    with pytest.raises(ValidationError):
        Workflow.model_validate_json("{not valid json")


def test_negative_estimated_cost_is_rejected() -> None:
    with pytest.raises(ValidationError, match="greater than or equal to 0"):
        Node(id=NODE_A, type="task", estimated_cost=-0.01)


def test_negative_max_cost_is_rejected() -> None:
    with pytest.raises(ValidationError, match="greater than or equal to 0"):
        WorkflowConstraints(max_cost=-1)


@pytest.mark.parametrize("max_latency_ms", [0, -100])
def test_non_positive_max_latency_is_rejected(max_latency_ms: int) -> None:
    with pytest.raises(ValidationError, match="greater than 0"):
        WorkflowConstraints(max_latency_ms=max_latency_ms)


@pytest.mark.parametrize("filename", ["linear.json", "branching.json"])
def test_json_round_trip(filename: str) -> None:
    original = Workflow.model_validate_json(
        (EXAMPLES_DIR / filename).read_text(encoding="utf-8")
    )

    dumped = original.model_dump(mode="json")
    assert isinstance(dumped["id"], str)
    assert Workflow.model_validate(dumped) == original
    assert Workflow.model_validate_json(original.model_dump_json()) == original


def test_schema_does_not_reject_multi_node_cycle() -> None:
    # Cycle detection belongs to the DAG validation milestone, not this schema.
    dag = DAG(
        nodes=[
            Node(id=NODE_A, type="task"),
            Node(id=NODE_B, type="task"),
            Node(id=NODE_C, type="task"),
        ],
        edges=[
            Edge(from_node=NODE_A, to_node=NODE_B),
            Edge(from_node=NODE_B, to_node=NODE_C),
            Edge(from_node=NODE_C, to_node=NODE_A),
        ],
    )

    workflow = Workflow(id=WORKFLOW_ID, name="Cycle", dag=dag)
    assert len(workflow.dag.edges) == 3
