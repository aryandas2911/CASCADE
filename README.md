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

Workflow execution, distributed services, APIs, persistence, and the other
runtime subsystems described in `docs/` and `adr/` are not implemented yet.

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
│       ├── models.py
│       └── schema.py
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
