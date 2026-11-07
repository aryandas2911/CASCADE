"""Typed workflow schemas and structural validation."""

from typing import Any, Self
from uuid import UUID

from pydantic import BaseModel, Field, model_validator


class Node(BaseModel):
    """A single unit of work inside a workflow DAG."""

    id: UUID
    type: str = Field(min_length=1)
    config: dict[str, Any] = Field(default_factory=dict)
    parallelizable: bool = False
    estimated_cost: float = Field(default=0, ge=0)


class Edge(BaseModel):
    """A directed connection between two workflow nodes."""

    from_node: UUID
    to_node: UUID


class DAG(BaseModel):
    """Workflow nodes and their directed edges."""

    nodes: list[Node] = Field(default_factory=list)
    edges: list[Edge] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_structure(self) -> Self:
        """Validate node uniqueness and edge references."""
        node_ids: set[UUID] = set()
        duplicate_ids: set[UUID] = set()

        for node in self.nodes:
            if node.id in node_ids:
                duplicate_ids.add(node.id)
            node_ids.add(node.id)

        if duplicate_ids:
            duplicates = ", ".join(sorted(str(node_id) for node_id in duplicate_ids))
            raise ValueError(f"duplicate node id(s): {duplicates}")

        seen_edges: set[tuple[UUID, UUID]] = set()

        for edge in self.edges:
            if edge.from_node == edge.to_node:
                raise ValueError(
                    f"self-referencing edge is not allowed: {edge.from_node}"
                )

            missing = [
                node_id
                for node_id in (edge.from_node, edge.to_node)
                if node_id not in node_ids
            ]
            if missing:
                missing_ids = ", ".join(sorted(str(node_id) for node_id in missing))
                raise ValueError(f"edge references unknown node id(s): {missing_ids}")

            edge_key = (edge.from_node, edge.to_node)
            if edge_key in seen_edges:
                raise ValueError(
                    "duplicate edge is not allowed: "
                    f"{edge.from_node} -> {edge.to_node}"
                )
            seen_edges.add(edge_key)

        return self


class WorkflowConstraints(BaseModel):
    """Optional resource limits for a workflow."""

    max_cost: float | None = Field(default=None, ge=0)
    max_latency_ms: int | None = Field(default=None, gt=0)


class Workflow(BaseModel):
    """A workflow definition with its DAG and constraints."""

    id: UUID
    name: str = Field(min_length=1)
    dag: DAG
    constraints: WorkflowConstraints = Field(default_factory=WorkflowConstraints)
