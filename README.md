# Cascade

Cascade is a distributed AI workflow execution engine designed to execute DAG-based
AI workflows while optimizing latency, cost, throughput, reliability, and
observability.

Cascade is infrastructure rather than an end-user AI application.

## Current Status

Early foundation. The package can be imported (``import cascade``) and started
(``python -m cascade``), which prints:

```text
Cascade runtime initialized
```

Typed workflow schemas are available for loading and structural validation.
Graph-level DAG validation, cycle detection, and deterministic topological
sorting are also available, along with read-only dependency analysis: execution
layers, node depths, and a cost-weighted critical path.

A synchronous local executor can run validated DAGs with registered Python
handlers. Distributed execution, asynchronous scheduling, retries, timeouts,
APIs, persistence, and the other runtime subsystems described in `docs/` and
`adr/` are not implemented yet.

## Requirements

- Python 3.11 or newer
- Git
- A terminal (PowerShell, Bash, or equivalent)

## Project Layout

```text
CASCADE/
├── src/cascade/
│   ├── __init__.py
│   ├── __main__.py
│   ├── planner/
│   │   ├── __init__.py
│   │   └── analysis.py
│   ├── runtime/
│   │   ├── __init__.py
│   │   ├── executor.py
│   │   ├── models.py
│   │   └── registry.py
│   └── workflow/
│       ├── __init__.py
│       ├── graph.py
│       └── schemas.py
├── examples/
│   ├── run_local_executor.py
│   └── workflows/
│       ├── branching.json
│       └── linear.json
├── tests/unit/
├── adr/
├── docs/
├── pyproject.toml
└── README.md
```

The package uses a `src/` layout so imports come from the installed package
rather than from the repository root.

## Setup

Create and activate a virtual environment from the repository root.

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

macOS or Linux:

```bash
python -m venv .venv
source .venv/bin/activate
```

Upgrade pip and install Cascade in editable mode with its development tools:

```bash
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

The `dev` extra installs pytest, Ruff, Black, and mypy.

## Running

Start the package from the repository root:

```bash
python -m cascade
```

Expected output:

```text
Cascade runtime initialized
```

The process exits with status code 0.

## Workflow Schemas

Pydantic models in `cascade.workflow` describe workflow definitions:

- `Workflow`: `id` (UUID), `name` (nonempty string), `dag`, optional `constraints`.
- `DAG`: `nodes` and directed `edges`.
- `Node`: `id` (UUID), `type` (nonempty string), `config` object,
  `parallelizable` flag, nonnegative `estimated_cost`.
- `Edge`: `from_node` and `to_node` node UUIDs.
- `WorkflowConstraints`: optional nonnegative `max_cost` and positive
  `max_latency_ms`.

Load an example:

```python
from pathlib import Path

from cascade.workflow import Workflow

workflow = Workflow.model_validate_json(
    Path("examples/workflows/branching.json").read_text(encoding="utf-8")
)
```

The schema layer validates node ID uniqueness, edge endpoints, self-referencing
edges, and duplicate edges. It deliberately accepts cyclic graphs so that
graph-level validation can report cycles explicitly.

## DAG Validation and Topological Sorting

Graph utilities in `cascade.workflow.graph` operate on an existing `DAG` without
mutating it:

```python
from pathlib import Path

from cascade.workflow import (
    DAGCycleError,
    Workflow,
    sink_nodes,
    source_nodes,
    topological_sort,
    validate_dag,
)

workflow = Workflow.model_validate_json(
    Path("examples/workflows/branching.json").read_text(encoding="utf-8")
)

try:
    validate_dag(workflow.dag)
except DAGCycleError as error:
    print(error.unresolved_node_ids)
else:
    order = topological_sort(workflow.dag)
    sources = source_nodes(workflow.dag)
    sinks = sink_nodes(workflow.dag)
```

`topological_sort` uses Kahn's algorithm and a min-heap keyed by `UUID.int`.
Ready nodes are therefore emitted in a reproducible order regardless of node or
edge insertion order. The cost is `O(V + E)` traversal plus `O(V log V)` heap
work.

A cycle raises `DAGCycleError`, a `DAGValidationError`. Its
`unresolved_node_ids` contains every node that could not be ordered. That set
includes nodes downstream of a cycle, so it is not necessarily exactly the
cycle members.

Empty DAGs return empty lists from `topological_sort`, `source_nodes`, and
`sink_nodes`. These utilities perform only dependency analysis; they do not
schedule or execute work.

## DAG Analysis and Execution Layers

`cascade.planner` provides read-only analysis on top of an existing `DAG`:

```python
from pathlib import Path

from cascade.planner import analyze_dag
from cascade.workflow import Workflow

workflow = Workflow.model_validate_json(
    Path("examples/workflows/branching.json").read_text(encoding="utf-8")
)

analysis = analyze_dag(workflow.dag)

for depth, layer in enumerate(analysis.execution_layers):
    print(depth, [str(node_id) for node_id in layer])

print(analysis.node_depths)
print(analysis.critical_path_nodes)
print(analysis.critical_path_estimated_cost)
```

For the branching example, layer 0 contains the shared upstream node and layer 1
contains its two downstream nodes. Nodes within each layer are sorted by
`UUID.int`, and every edge `(u, v)` satisfies `depth[u] < depth[v]`.

- `execution_layers(dag)` groups nodes by dependency depth. Layers express
  eligibility only; they do not guarantee simultaneous execution or define a
  scheduling policy. `Node.parallelizable` remains metadata and is not
  interpreted here.
- `node_depths(dag)` assigns depth 0 to sources and
  `1 + max(predecessor depths)` to every other node, including joins and
  disconnected components.
- `critical_cost_path(dag)` returns a `CriticalCostPath` with
  `estimated_cost` summed along the maximum-cost path.

**Important:** `estimated_cost` represents estimated monetary or resource cost,
not duration. The result is therefore a **cost-weighted critical path, not an
execution-time critical path or latency prediction**. `max_latency_ms` is never
used as a node weight; latency analysis needs separately defined duration
estimates.

Equal-cost alternatives are resolved by the lexicographically smallest complete
sequence of `UUID.int` values, so results do not depend on node or edge
insertion order. Zero-cost nodes are still included in paths.

`analyze_dag(dag)` returns an immutable `DAGAnalysis` containing
`execution_layers`, `node_depths`, `critical_path_nodes`, and
`critical_path_estimated_cost`. All analysis functions propagate `DAGCycleError`
instead of returning partial results. An empty DAG produces empty layers,
depths, and path, with cost `0.0`.

The analysis runs in `O(V + E)` graph traversal plus deterministic ordering
overhead (`O(V log V)` in the worst case).

## Local Sequential Executor

`cascade.runtime.LocalExecutor` runs a validated workflow synchronously with
handlers registered in an injected `TaskRegistry`:

```python
from typing import Any
from uuid import UUID

from cascade.runtime import LocalExecutor, TaskRegistry
from cascade.workflow import Workflow

registry = TaskRegistry()


def constant(config: dict[str, Any], inputs: dict[UUID, Any]) -> Any:
    return config["value"]


def multiply(config: dict[str, Any], inputs: dict[UUID, Any]) -> Any:
    (value,) = inputs.values()
    return value * config["factor"]


registry.register("constant", constant)
registry.register("multiply", multiply)

result = LocalExecutor(registry).execute(workflow)
```

Each handler receives its node's `config` and a dictionary of **direct
predecessor outputs** keyed by predecessor `UUID`. Outputs are arbitrary Python
objects held in memory; they are not serialized.

Execution behavior:

- The entire DAG is validated before any handler runs. A cycle raises
  `DAGCycleError`.
- Nodes run sequentially in deterministic topological order.
- A successful workflow returns `WorkflowStatus.COMPLETED`.
- An unknown handler or handler exception marks that node `FAILED`, marks the
  workflow `FAILED`, preserves earlier outputs, leaves unstarted nodes
  `PENDING`, and stops immediately. Error details remain on the node result.
- Each execution creates fresh result objects; repeated runs do not reuse
  previous state.
- Empty DAGs complete successfully with no node results.

`TaskRegistry` rejects duplicate node types and reports unknown types
explicitly. It is a per-executor object, not a global singleton.

Run the three-node constant → multiply → combine demo:

```bash
python examples/run_local_executor.py
```

This executor is intentionally local and synchronous. It has no asyncio,
threads, retries, timeouts, cancellation, checkpointing, or distributed
workers.

## Tests

Run the unit test suite:

```bash
python -m pytest
```

Run a single test file:

```bash
python -m pytest tests/unit/test_entrypoint.py
```

Tests are configured with `testpaths = ["tests"]` in `pyproject.toml`.

## Linting

Check formatting and lint rules with Ruff:

```bash
python -m ruff check .
```

Apply safe automatic fixes:

```bash
python -m ruff check --fix .
```

## Development Tooling

The `pyproject.toml` file configures:

- setuptools packaging with a `src/` layout
- pytest test discovery
- Ruff linting
- Black and mypy development dependencies

## Engineering Principles

- Correctness before optimization
- Reliability before peak performance
- Explainable runtime decisions
- Modular architecture
- Benchmark-driven optimization
- Progressive testing
- Production-oriented engineering

## License

See `LICENSE`.
