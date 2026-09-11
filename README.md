# Cascade

Cascade is a distributed AI workflow execution engine designed to execute DAG-based AI workflows while optimizing latency, cost, throughput, reliability, and observability.

Cascade is infrastructure rather than an end-user AI application.

## Current Status

Early development.

The project is being implemented incrementally, beginning with the workflow and local execution foundations before introducing distributed execution, reliability mechanisms, optimization, observability, replay, and production deployment.

## Core Architecture

Client
→ API Gateway
→ Planner
→ Scheduler
→ Queue
→ Workers
→ Model Router

Supporting systems include persistent state, checkpointing, caching, observability, replay, benchmarking, SDKs, CLI, and dashboard components.

## Engineering Principles

- Correctness before optimization
- Reliability before peak performance
- Explainable runtime decisions
- Modular architecture
- Benchmark-driven optimization
- Progressive testing
- Production-oriented engineering

## Development

The project uses Python for the core runtime and follows a `src/` package layout.

Development tooling includes:

- pytest
- Ruff
- Black
- mypy

More detailed architecture and development documentation will be added as the corresponding subsystems are implemented.
