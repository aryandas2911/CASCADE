"""Graph-level DAG validation and deterministic traversal utilities.

These helpers operate on the structural ``DAG`` schema. They do not schedule,
execute, or otherwise mutate workflow definitions.
"""

import heapq
from uuid import UUID

from cascade.workflow.schemas import DAG


class DAGValidationError(ValueError):
    """Base error for graph-level DAG validation failures."""


class DAGCycleError(DAGValidationError):
    """Raised when a DAG cannot be topologically ordered because of a cycle."""

    def __init__(self, unresolved_node_ids: set[UUID]) -> None:
        self.unresolved_node_ids: tuple[UUID, ...] = tuple(
            sorted(unresolved_node_ids, key=lambda node_id: node_id.int)
        )
        unresolved = ", ".join(str(node_id) for node_id in self.unresolved_node_ids)
        super().__init__(
            "DAG contains a cycle; "
            f"{len(self.unresolved_node_ids)} node(s) could not be ordered: "
            f"{unresolved}"
        )


def _build_adjacency(
    dag: DAG,
) -> tuple[dict[UUID, list[UUID]], dict[UUID, int]]:
    """Build adjacency lists and indegree counts in O(V + E)."""
    adjacency: dict[UUID, list[UUID]] = {node.id: [] for node in dag.nodes}
    indegree: dict[UUID, int] = {node.id: 0 for node in dag.nodes}

    for edge in dag.edges:
        adjacency[edge.from_node].append(edge.to_node)
        indegree[edge.to_node] += 1

    return adjacency, indegree


def _by_uuid_int(node_id: UUID) -> int:
    return node_id.int


def topological_sort(dag: DAG) -> list[UUID]:
    """Return a deterministic topological order using Kahn's algorithm.

    Nodes that become ready at the same time are ordered by ``UUID.int`` via a
    min-heap, so the result does not depend on node or edge insertion order.

    Raises:
        DAGCycleError: If the graph contains a cycle.
    """
    adjacency, indegree = _build_adjacency(dag)

    ready: list[tuple[int, UUID]] = [
        (node_id.int, node_id)
        for node_id, count in indegree.items()
        if count == 0
    ]
    heapq.heapify(ready)

    order: list[UUID] = []

    while ready:
        _, node_id = heapq.heappop(ready)
        order.append(node_id)

        for downstream_id in adjacency[node_id]:
            indegree[downstream_id] -= 1
            if indegree[downstream_id] == 0:
                heapq.heappush(ready, (downstream_id.int, downstream_id))

    if len(order) != len(dag.nodes):
        # Unresolved nodes include cycle members and anything downstream of them,
        # so they are not necessarily all part of a cycle.
        unresolved = set(adjacency).difference(order)
        raise DAGCycleError(unresolved)

    return order


def validate_dag(dag: DAG) -> None:
    """Validate that the DAG can be topologically ordered.

    Structural rules (unique node IDs, known edge endpoints, no self-edges, no
    duplicate edges) are enforced by the schema. This function adds the
    graph-level acyclicity check.

    Raises:
        DAGCycleError: If the graph contains a cycle.
    """
    topological_sort(dag)


def source_nodes(dag: DAG) -> list[UUID]:
    """Return node IDs with no incoming edges, ordered by ``UUID.int``."""
    _, indegree = _build_adjacency(dag)
    return sorted(
        (node_id for node_id, count in indegree.items() if count == 0),
        key=_by_uuid_int,
    )


def sink_nodes(dag: DAG) -> list[UUID]:
    """Return node IDs with no outgoing edges, ordered by ``UUID.int``."""
    adjacency, _ = _build_adjacency(dag)
    return sorted(
        (node_id for node_id, downstream in adjacency.items() if not downstream),
        key=_by_uuid_int,
    )
