from collections.abc import Callable
from dataclasses import FrozenInstanceError
from itertools import permutations
from uuid import UUID

import pytest

from cascade.planner import (
    DAGAnalysis,
    analyze_dag,
    critical_cost_path,
    execution_layers,
    node_depths,
)
from cascade.workflow import DAG, DAGCycleError, Edge, Node


def _node_id(value: int) -> UUID:
    return UUID(f"70000000-0000-4000-8000-{value:012d}")


NODE_A = _node_id(1)
NODE_B = _node_id(2)
NODE_C = _node_id(3)
NODE_D = _node_id(4)
NODE_E = _node_id(5)


def _make_dag(
    nodes: list[tuple[UUID, float]],
    edges: list[tuple[UUID, UUID]],
) -> DAG:
    return DAG(
        nodes=[
            Node(id=node_id, type="task", estimated_cost=cost)
            for node_id, cost in nodes
        ],
        edges=[
            Edge(from_node=from_node, to_node=to_node)
            for from_node, to_node in edges
        ],
    )


def _assert_layers_are_valid(dag: DAG, layers: list[list[UUID]]) -> None:
    flattened = [node_id for layer in layers for node_id in layer]
    assert len(flattened) == len(dag.nodes)
    assert set(flattened) == {node.id for node in dag.nodes}
    assert len(set(flattened)) == len(flattened)

    depth = {
        node_id: layer_index
        for layer_index, layer in enumerate(layers)
        for node_id in layer
    }
    for edge in dag.edges:
        assert depth[edge.from_node] < depth[edge.to_node]

    for layer in layers:
        assert layer
        assert layer == sorted(layer, key=lambda node_id: node_id.int)


def _assert_valid_cost_path(
    dag: DAG,
    nodes: tuple[UUID, ...],
    cost: float,
) -> None:
    if not nodes:
        assert cost == 0.0
        return

    node_by_id = {node.id: node for node in dag.nodes}
    assert len(set(nodes)) == len(nodes)
    assert all(node_id in node_by_id for node_id in nodes)

    edge_pairs = {(edge.from_node, edge.to_node) for edge in dag.edges}
    for from_node, to_node in zip(nodes, nodes[1:], strict=False):
        assert (from_node, to_node) in edge_pairs

    predecessors = {edge.to_node for edge in dag.edges}
    assert nodes[0] not in predecessors

    total = sum(node_by_id[node_id].estimated_cost for node_id in nodes)
    assert total == pytest.approx(cost)


def test_empty_dag_analysis() -> None:
    dag = DAG()

    assert execution_layers(dag) == []
    assert node_depths(dag) == {}
    path = critical_cost_path(dag)
    assert path.nodes == ()
    assert path.estimated_cost == 0.0

    analysis = analyze_dag(dag)
    assert analysis.execution_layers == ()
    assert dict(analysis.node_depths) == {}
    assert analysis.critical_path_nodes == ()
    assert analysis.critical_path_estimated_cost == 0.0


def test_single_node_is_one_layer_and_full_path() -> None:
    dag = _make_dag([(NODE_A, 2.5)], [])

    layers = execution_layers(dag)
    assert layers == [[NODE_A]]
    assert node_depths(dag) == {NODE_A: 0}

    path = critical_cost_path(dag)
    assert path.nodes == (NODE_A,)
    assert path.estimated_cost == 2.5
    _assert_layers_are_valid(dag, layers)
    _assert_valid_cost_path(dag, path.nodes, path.estimated_cost)


def test_linear_chain_layers_depths_and_total_cost() -> None:
    dag = _make_dag(
        [(NODE_C, 3.0), (NODE_A, 1.0), (NODE_B, 2.0)],
        [(NODE_B, NODE_C), (NODE_A, NODE_B)],
    )

    layers = execution_layers(dag)
    assert layers == [[NODE_A], [NODE_B], [NODE_C]]
    assert node_depths(dag) == {NODE_A: 0, NODE_B: 1, NODE_C: 2}

    path = critical_cost_path(dag)
    assert path.nodes == (NODE_A, NODE_B, NODE_C)
    assert path.estimated_cost == 6.0
    _assert_layers_are_valid(dag, layers)
    _assert_valid_cost_path(dag, path.nodes, path.estimated_cost)


def test_diamond_dag_prefers_higher_cost_branch() -> None:
    dag = _make_dag(
        [(NODE_A, 1.0), (NODE_B, 2.0), (NODE_C, 5.0), (NODE_D, 1.0)],
        [
            (NODE_A, NODE_B),
            (NODE_A, NODE_C),
            (NODE_B, NODE_D),
            (NODE_C, NODE_D),
        ],
    )

    layers = execution_layers(dag)
    assert layers == [[NODE_A], [NODE_B, NODE_C], [NODE_D]]
    assert node_depths(dag) == {
        NODE_A: 0,
        NODE_B: 1,
        NODE_C: 1,
        NODE_D: 2,
    }

    path = critical_cost_path(dag)
    assert path.nodes == (NODE_A, NODE_C, NODE_D)
    assert path.estimated_cost == 7.0
    _assert_layers_are_valid(dag, layers)
    _assert_valid_cost_path(dag, path.nodes, path.estimated_cost)


def test_disconnected_components_are_analyzed_together() -> None:
    dag = _make_dag(
        [
            (NODE_A, 1.0),
            (NODE_B, 1.0),
            (NODE_C, 4.0),
            (NODE_D, 1.0),
            (NODE_E, 10.0),
        ],
        [(NODE_A, NODE_B), (NODE_C, NODE_D)],
    )

    layers = execution_layers(dag)
    assert layers == [[NODE_A, NODE_C, NODE_E], [NODE_B, NODE_D]]
    assert node_depths(dag) == {
        NODE_A: 0,
        NODE_B: 1,
        NODE_C: 0,
        NODE_D: 1,
        NODE_E: 0,
    }

    path = critical_cost_path(dag)
    assert path.nodes == (NODE_E,)
    assert path.estimated_cost == 10.0
    _assert_layers_are_valid(dag, layers)
    _assert_valid_cost_path(dag, path.nodes, path.estimated_cost)


def test_node_with_multiple_dependencies_uses_deepest_parent() -> None:
    dag = _make_dag(
        [(NODE_A, 1.0), (NODE_B, 5.0), (NODE_C, 1.0), (NODE_D, 1.0)],
        [
            (NODE_A, NODE_C),
            (NODE_B, NODE_C),
            (NODE_C, NODE_D),
            (NODE_B, NODE_D),
        ],
    )

    layers = execution_layers(dag)
    assert layers == [[NODE_A, NODE_B], [NODE_C], [NODE_D]]
    assert node_depths(dag) == {
        NODE_A: 0,
        NODE_B: 0,
        NODE_C: 1,
        NODE_D: 2,
    }

    path = critical_cost_path(dag)
    assert path.nodes == (NODE_B, NODE_C, NODE_D)
    assert path.estimated_cost == 7.0
    _assert_layers_are_valid(dag, layers)
    _assert_valid_cost_path(dag, path.nodes, path.estimated_cost)


def test_unequal_costs_select_maximum_cumulative_path() -> None:
    dag = _make_dag(
        [(NODE_A, 1.0), (NODE_B, 1.0), (NODE_C, 10.0), (NODE_D, 1.0)],
        [
            (NODE_A, NODE_B),
            (NODE_A, NODE_C),
            (NODE_B, NODE_D),
            (NODE_C, NODE_D),
        ],
    )

    path = critical_cost_path(dag)

    assert path.nodes == (NODE_A, NODE_C, NODE_D)
    assert path.estimated_cost == 12.0
    _assert_valid_cost_path(dag, path.nodes, path.estimated_cost)


def test_equal_cost_paths_use_lexicographic_uuid_sequence() -> None:
    node_orders = [
        [(NODE_A, 1.0), (NODE_B, 1.0), (NODE_C, 1.0), (NODE_D, 1.0)],
        [(NODE_D, 1.0), (NODE_C, 1.0), (NODE_B, 1.0), (NODE_A, 1.0)],
    ]
    edges = [
        (NODE_A, NODE_B),
        (NODE_A, NODE_C),
        (NODE_B, NODE_D),
        (NODE_C, NODE_D),
    ]

    for nodes in node_orders:
        for edge_order in (edges, list(reversed(edges))):
            dag = _make_dag(nodes, edge_order)
            path = critical_cost_path(dag)

            # (A, B, D) is lexicographically smaller than (A, C, D).
            assert path.nodes == (NODE_A, NODE_B, NODE_D)
            assert path.estimated_cost == 3.0
            _assert_valid_cost_path(dag, path.nodes, path.estimated_cost)


def test_equal_cost_endpoints_use_complete_lexicographic_path() -> None:
    # Path (A, D) and path (B, C) both cost 5. (1, 4) < (2, 3).
    dag = _make_dag(
        [(NODE_A, 0.0), (NODE_B, 0.0), (NODE_C, 5.0), (NODE_D, 5.0)],
        [(NODE_A, NODE_D), (NODE_B, NODE_C)],
    )

    path = critical_cost_path(dag)

    assert path.nodes == (NODE_A, NODE_D)
    assert path.estimated_cost == 5.0
    _assert_valid_cost_path(dag, path.nodes, path.estimated_cost)


def test_all_zero_weights_choose_lexicographically_smallest_path() -> None:
    dag = _make_dag(
        [(NODE_A, 0.0), (NODE_B, 0.0), (NODE_C, 0.0), (NODE_D, 0.0)],
        [
            (NODE_A, NODE_B),
            (NODE_A, NODE_C),
            (NODE_B, NODE_D),
            (NODE_C, NODE_D),
        ],
    )

    path = critical_cost_path(dag)

    # The single-node path (1,) is a prefix of every extended path from A.
    assert path.nodes == (NODE_A,)
    assert path.estimated_cost == 0.0
    _assert_valid_cost_path(dag, path.nodes, path.estimated_cost)


@pytest.mark.parametrize(
    "analyze",
    [analyze_dag, critical_cost_path, execution_layers, node_depths],
)
def test_cyclic_dag_is_rejected(analyze: Callable[[DAG], object]) -> None:
    dag = _make_dag(
        [(NODE_A, 1.0), (NODE_B, 1.0)],
        [(NODE_A, NODE_B), (NODE_B, NODE_A)],
    )

    with pytest.raises(DAGCycleError):
        analyze(dag)


def test_cyclic_dag_is_rejected_for_three_node_cycle() -> None:
    dag = _make_dag(
        [(NODE_A, 1.0), (NODE_B, 1.0), (NODE_C, 1.0)],
        [(NODE_A, NODE_B), (NODE_B, NODE_C), (NODE_C, NODE_A)],
    )

    with pytest.raises(DAGCycleError):
        analyze_dag(dag)


def test_results_are_deterministic_under_shuffled_insertion() -> None:
    nodes = [
        (NODE_A, 1.0),
        (NODE_B, 2.0),
        (NODE_C, 1.0),
        (NODE_D, 3.0),
    ]
    edges = [
        (NODE_A, NODE_B),
        (NODE_A, NODE_C),
        (NODE_B, NODE_D),
        (NODE_C, NODE_D),
    ]

    baseline = analyze_dag(_make_dag(nodes, edges))
    edge_orders = [
        edges,
        list(reversed(edges)),
        [edges[2], edges[0], edges[3], edges[1]],
    ]

    for node_order in permutations(nodes):
        for edge_order in edge_orders:
            dag = _make_dag(list(node_order), edge_order)
            analysis = analyze_dag(dag)

            assert analysis == baseline
            assert critical_cost_path(dag) == critical_cost_path(
                _make_dag(nodes, edges)
            )
            assert execution_layers(dag) == execution_layers(
                _make_dag(nodes, edges)
            )
            assert node_depths(dag) == node_depths(_make_dag(nodes, edges))


def test_analysis_does_not_mutate_the_input_dag() -> None:
    dag = _make_dag(
        [(NODE_A, 1.0), (NODE_B, 2.0), (NODE_C, 3.0)],
        [(NODE_A, NODE_B), (NODE_B, NODE_C)],
    )
    before = dag.model_dump(mode="json")
    node_list_identity = id(dag.nodes)
    edge_list_identity = id(dag.edges)
    node_identities = [id(node) for node in dag.nodes]
    edge_identities = [id(edge) for edge in dag.edges]

    execution_layers(dag)
    node_depths(dag)
    critical_cost_path(dag)
    analyze_dag(dag)

    assert dag.model_dump(mode="json") == before
    assert id(dag.nodes) == node_list_identity
    assert id(dag.edges) == edge_list_identity
    assert [id(node) for node in dag.nodes] == node_identities
    assert [id(edge) for edge in dag.edges] == edge_identities


def test_analyze_dag_matches_individual_functions() -> None:
    dag = _make_dag(
        [(NODE_A, 1.0), (NODE_B, 4.0), (NODE_C, 2.0), (NODE_D, 1.0)],
        [
            (NODE_A, NODE_B),
            (NODE_A, NODE_C),
            (NODE_B, NODE_D),
            (NODE_C, NODE_D),
        ],
    )

    analysis = analyze_dag(dag)
    path = critical_cost_path(dag)

    assert analysis.execution_layers == tuple(
        tuple(layer) for layer in execution_layers(dag)
    )
    assert dict(analysis.node_depths) == node_depths(dag)
    assert analysis.critical_path_nodes == path.nodes
    assert analysis.critical_path_estimated_cost == path.estimated_cost
    _assert_layers_are_valid(dag, [list(layer) for layer in analysis.execution_layers])
    _assert_valid_cost_path(
        dag,
        analysis.critical_path_nodes,
        analysis.critical_path_estimated_cost,
    )


def test_analysis_result_and_path_are_immutable() -> None:
    dag = _make_dag([(NODE_A, 1.0)], [])
    analysis = analyze_dag(dag)
    path = critical_cost_path(dag)

    assert isinstance(analysis, DAGAnalysis)
    assert isinstance(analysis.execution_layers, tuple)
    assert isinstance(analysis.execution_layers[0], tuple)
    assert path.nodes == (NODE_A,)

    with pytest.raises(FrozenInstanceError):
        analysis.critical_path_estimated_cost = 2.0

    with pytest.raises(TypeError):
        analysis.node_depths[NODE_A] = 99

    with pytest.raises(FrozenInstanceError):
        path.estimated_cost = 2.0
