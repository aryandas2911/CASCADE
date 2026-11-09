from itertools import permutations
from pathlib import Path
from uuid import UUID

import pytest

from cascade.workflow import (
    DAG,
    DAGCycleError,
    DAGValidationError,
    Edge,
    Node,
    Workflow,
    sink_nodes,
    source_nodes,
    topological_sort,
    validate_dag,
)

EXAMPLES_DIR = Path(__file__).resolve().parents[3] / "examples" / "workflows"


def _node_id(value: int) -> UUID:
    return UUID(f"60000000-0000-4000-8000-{value:012d}")


NODE_A = _node_id(1)
NODE_B = _node_id(2)
NODE_C = _node_id(3)
NODE_D = _node_id(4)
NODE_E = _node_id(5)


def _make_dag(
    node_ids: list[UUID],
    edges: list[tuple[UUID, UUID]],
) -> DAG:
    return DAG(
        nodes=[Node(id=node_id, type="task") for node_id in node_ids],
        edges=[
            Edge(from_node=from_node, to_node=to_node)
            for from_node, to_node in edges
        ],
    )


def _assert_is_topological_order(dag: DAG, order: list[UUID]) -> None:
    position = {node_id: index for index, node_id in enumerate(order)}
    assert set(position) == {node.id for node in dag.nodes}
    for edge in dag.edges:
        assert position[edge.from_node] < position[edge.to_node]


def test_empty_dag_returns_empty_graph_results() -> None:
    dag = DAG()

    assert topological_sort(dag) == []
    assert source_nodes(dag) == []
    assert sink_nodes(dag) == []
    assert validate_dag(dag) is None


def test_single_node_is_both_source_and_sink() -> None:
    dag = _make_dag([NODE_A], [])

    assert topological_sort(dag) == [NODE_A]
    assert source_nodes(dag) == [NODE_A]
    assert sink_nodes(dag) == [NODE_A]


def test_linear_chain_is_ordered_regardless_of_insertion_order() -> None:
    dag = _make_dag(
        [NODE_C, NODE_A, NODE_B],
        [(NODE_B, NODE_C), (NODE_A, NODE_B)],
    )

    order = topological_sort(dag)

    assert order == [NODE_A, NODE_B, NODE_C]
    assert source_nodes(dag) == [NODE_A]
    assert sink_nodes(dag) == [NODE_C]
    _assert_is_topological_order(dag, order)


def test_diamond_dag_is_ordered_and_branches_converge() -> None:
    dag = _make_dag(
        [NODE_A, NODE_B, NODE_C, NODE_D],
        [
            (NODE_A, NODE_B),
            (NODE_A, NODE_C),
            (NODE_B, NODE_D),
            (NODE_C, NODE_D),
        ],
    )

    order = topological_sort(dag)

    assert order == [NODE_A, NODE_B, NODE_C, NODE_D]
    assert source_nodes(dag) == [NODE_A]
    assert sink_nodes(dag) == [NODE_D]
    _assert_is_topological_order(dag, order)


def test_disconnected_components_are_all_ordered() -> None:
    dag = _make_dag(
        [NODE_A, NODE_B, NODE_C, NODE_D, NODE_E],
        [(NODE_A, NODE_B), (NODE_C, NODE_D)],
    )

    order = topological_sort(dag)

    assert order == [NODE_A, NODE_B, NODE_C, NODE_D, NODE_E]
    assert source_nodes(dag) == [NODE_A, NODE_C, NODE_E]
    assert sink_nodes(dag) == [NODE_B, NODE_D, NODE_E]
    _assert_is_topological_order(dag, order)


def test_multiple_sources_and_sinks_are_sorted_by_uuid_int() -> None:
    dag = _make_dag(
        [NODE_E, NODE_D, NODE_C, NODE_B, NODE_A],
        [
            (NODE_B, NODE_C),
            (NODE_A, NODE_C),
            (NODE_C, NODE_E),
            (NODE_C, NODE_D),
        ],
    )

    assert source_nodes(dag) == [NODE_A, NODE_B]
    assert sink_nodes(dag) == [NODE_D, NODE_E]
    _assert_is_topological_order(dag, topological_sort(dag))


def test_two_node_cycle_raises_dag_cycle_error() -> None:
    dag = _make_dag([NODE_A, NODE_B], [(NODE_A, NODE_B), (NODE_B, NODE_A)])

    with pytest.raises(DAGCycleError) as exc_info:
        topological_sort(dag)

    assert exc_info.value.unresolved_node_ids == (NODE_A, NODE_B)
    assert str(NODE_A) in str(exc_info.value)
    assert str(NODE_B) in str(exc_info.value)


def test_three_node_cycle_raises_dag_cycle_error() -> None:
    dag = _make_dag(
        [NODE_A, NODE_B, NODE_C],
        [(NODE_A, NODE_B), (NODE_B, NODE_C), (NODE_C, NODE_A)],
    )

    with pytest.raises(DAGCycleError) as exc_info:
        validate_dag(dag)

    assert exc_info.value.unresolved_node_ids == (NODE_A, NODE_B, NODE_C)


def test_unresolved_nodes_are_not_all_claimed_to_be_in_the_cycle() -> None:
    dag = _make_dag(
        [NODE_A, NODE_B, NODE_C],
        [(NODE_A, NODE_B), (NODE_B, NODE_A), (NODE_B, NODE_C)],
    )

    with pytest.raises(DAGCycleError) as exc_info:
        topological_sort(dag)

    error = exc_info.value
    # NODE_C is downstream of the cycle, not necessarily part of it.
    assert error.unresolved_node_ids == (NODE_A, NODE_B, NODE_C)
    assert "could not be ordered" in str(error)


def test_cycle_error_is_a_graph_validation_error() -> None:
    dag = _make_dag([NODE_A, NODE_B], [(NODE_A, NODE_B), (NODE_B, NODE_A)])

    with pytest.raises(DAGValidationError):
        topological_sort(dag)


def test_ordering_is_deterministic_across_runs_and_insertion_orders() -> None:
    node_ids = [NODE_A, NODE_B, NODE_C, NODE_D, NODE_E]
    edges = [
        (NODE_A, NODE_C),
        (NODE_B, NODE_C),
        (NODE_C, NODE_D),
        (NODE_C, NODE_E),
    ]

    expected_order = [NODE_A, NODE_B, NODE_C, NODE_D, NODE_E]
    expected_sources = [NODE_A, NODE_B]
    expected_sinks = [NODE_D, NODE_E]

    for node_order in permutations(node_ids):
        for edge_order in (edges, list(reversed(edges))):
            dag = _make_dag(list(node_order), edge_order)

            assert topological_sort(dag) == expected_order
            assert source_nodes(dag) == expected_sources
            assert sink_nodes(dag) == expected_sinks

    dag = _make_dag(node_ids, edges)
    assert all(topological_sort(dag) == expected_order for _ in range(10))


def test_graph_utilities_do_not_mutate_the_input_dag() -> None:
    dag = _make_dag(
        [NODE_A, NODE_B, NODE_C],
        [(NODE_A, NODE_B), (NODE_B, NODE_C)],
    )
    before = dag.model_dump(mode="json")
    node_list_identity = id(dag.nodes)
    edge_list_identity = id(dag.edges)

    topological_sort(dag)
    validate_dag(dag)
    source_nodes(dag)
    sink_nodes(dag)

    assert dag.model_dump(mode="json") == before
    assert id(dag.nodes) == node_list_identity
    assert id(dag.edges) == edge_list_identity


@pytest.mark.parametrize("filename", ["linear.json", "branching.json"])
def test_example_workflows_have_valid_topological_orders(filename: str) -> None:
    workflow = Workflow.model_validate_json(
        (EXAMPLES_DIR / filename).read_text(encoding="utf-8")
    )

    assert validate_dag(workflow.dag) is None
    order = topological_sort(workflow.dag)
    _assert_is_topological_order(workflow.dag, order)
