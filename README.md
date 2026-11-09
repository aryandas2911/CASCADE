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
sorting are also available. Scheduling, workflow execution, distributed
services, APIs, persistence, and the other runtime subsystems described in
`docs/` and `adr/` are not implemented yet.

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
│   ├── runtime/
│   │   └── __init__.py
│   └── workflow/
│       ├── __init__.py
│       ├── graph.py
│       └── schemas.py
├── examples/workflows/
│   ├── branching.json
│   └── linear.json
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
