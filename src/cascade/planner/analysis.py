"""Deterministic, read-only DAG analysis for future local planning.

``Node.estimated_cost`` represents estimated monetary or resource cost, not
duration. The cost-weighted critical path computed here is therefore **not** an
execution-time critical path and must not be used as a latency prediction.
Latency-based analysis would require separately defined duration estimates;
``WorkflowConstraints.max_latency_ms`` is never used as a node weight.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from uuid import UUID

from cascade.workflow.graph import topological_sort
from cascade.workflow.schemas import DAG


@dataclass(frozen=True, slots=True)
class CriticalCostPath:
    """A maximum estimated-cost path through a DAG.

    Despite the historical "critical path" name, this is cost-weighted and is
    not a duration or latency estimate.
    """

    nodes: tuple[UUID, ...]
    estimated_cost: float


@dataclass(frozen=True, slots=True)
class DAGAnalysis:
    """Immutable result of read-only DAG dependency and cost analysis."""

    execution_layers: tuple[tuple[UUID, ...], ...]
    node_depths: Mapping[UUID, int]
    critical_path_nodes: tuple[UUID, ...]
    critical_path_estimated_cost: float


def _predecessors(dag: DAG) -> dict[UUID, list[UUID]]:
    """Build predecessor lists in O(V + E)."""
    predecessors: dict[UUID, list[UUID]] = {node.id: [] for node in dag.nodes}
    for edge in dag.edges:
        predecessors[edge.to_node].append(edge.from_node)
    return predecessors


def node_depths(dag: DAG) -> dict[UUID, int]:
    """Return each node's dependency depth.

    A source node has depth 0. Every other node has depth
    ``1 + max(predecessor depths)``. Disconnected components and joins are
    handled naturally because nodes are processed in topological order.

    Raises:
        DAGCycleError: If the graph contains a cycle.
    """
    predecessors = _predecessors(dag)
    depths: dict[UUID, int] = {}

    for node_id in topological_sort(dag):
        incoming = predecessors[node_id]
        depths[node_id] = (
            0 if not incoming else 1 + max(depths[parent] for parent in incoming)
        )

    return depths


def _layers_from_depths(depths: Mapping[UUID, int]) -> list[list[UUID]]:
    if not depths:
        return []

    layers: list[list[UUID]] = [[] for _ in range(max(depths.values()) + 1)]
    for node_id, depth in depths.items():
        layers[depth].append(node_id)

    for layer in layers:
        layer.sort(key=lambda node_id: node_id.int)

    return layers


def execution_layers(dag: DAG) -> list[list[UUID]]:
    """Group nodes into dependency layers, ordered by ``UUID.int``.

    Layer 0 contains source nodes. Every edge ``(u, v)`` has
    ``depth[u] < depth[v]``, so each layer contains nodes whose dependencies
    are all in earlier layers. Layers express dependency eligibility only; they
    do not guarantee simultaneous execution or apply resource scheduling policy.

    Raises:
        DAGCycleError: If the graph contains a cycle.
    """
    return _layers_from_depths(node_depths(dag))


def critical_cost_path(dag: DAG) -> CriticalCostPath:
    """Return the maximum estimated-cost path through the DAG.

    Node weights are ``Node.estimated_cost``. For each node, the maximum
    cumulative weight of any source-to-node path (including the node itself) is
    computed in topological order. Equal-weight alternatives are broken by the
    lexicographically smallest complete sequence of ``UUID.int`` values, so the
    result does not depend on input order.

    This is **not** an execution-time or latency critical path.

    Raises:
        DAGCycleError: If the graph contains a cycle.
    """
    order = topological_sort(dag)
    if not order:
        return CriticalCostPath(nodes=(), estimated_cost=0.0)

    costs = {node.id: node.estimated_cost for node in dag.nodes}
    predecessors = _predecessors(dag)
    best_cost: dict[UUID, float] = {}
    best_path: dict[UUID, tuple[int, ...]] = {}

    for node_id in order:
        weight = costs[node_id]
        incoming = predecessors[node_id]

        if not incoming:
            best_cost[node_id] = weight
            best_path[node_id] = (node_id.int,)
            continue

        chosen: tuple[float, tuple[int, ...]] | None = None
        for parent in incoming:
            candidate = (
                best_cost[parent] + weight,
                best_path[parent] + (node_id.int,),
            )
            if chosen is None or candidate[0] > chosen[0]:
                chosen = candidate
            elif candidate[0] == chosen[0] and candidate[1] < chosen[1]:
                chosen = candidate

        assert chosen is not None  # Non-sources always have a predecessor.
        best_cost[node_id] = chosen[0]
        best_path[node_id] = chosen[1]

    endpoint = order[0]
    for node_id in order[1:]:
        if best_cost[node_id] > best_cost[endpoint]:
            endpoint = node_id
        elif (
            best_cost[node_id] == best_cost[endpoint]
            and best_path[node_id] < best_path[endpoint]
        ):
            endpoint = node_id

    node_by_int = {node.id.int: node.id for node in dag.nodes}
    nodes = tuple(node_by_int[key] for key in best_path[endpoint])
    return CriticalCostPath(nodes=nodes, estimated_cost=best_cost[endpoint])


def analyze_dag(dag: DAG) -> DAGAnalysis:
    """Run dependency, layer, and cost-path analysis on a DAG.

    The result is immutable. Cycle errors from the underlying graph traversal
    propagate; no partial analysis is returned.

    Raises:
        DAGCycleError: If the graph contains a cycle.
    """
    depths = node_depths(dag)
    layers = _layers_from_depths(depths)
    cost_path = critical_cost_path(dag)

    return DAGAnalysis(
        execution_layers=tuple(tuple(layer) for layer in layers),
        node_depths=MappingProxyType(dict(depths)),
        critical_path_nodes=cost_path.nodes,
        critical_path_estimated_cost=cost_path.estimated_cost,
    )
