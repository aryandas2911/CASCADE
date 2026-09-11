# ADR 0001: Cascade Architecture

## Status

Accepted

## Context

Cascade is a distributed AI workflow execution engine that must execute DAG-based workflows while providing scheduling, reliability, observability, and extensibility.

The system requires clear separation between workflow planning, scheduling, execution, workers, persistence, and supporting infrastructure.

## Decision

Cascade will use a modular architecture with independently replaceable subsystems connected through well-defined interfaces.

The primary execution flow is:

Client
→ API Gateway
→ Planner
→ Scheduler
→ Queue
→ Workers
→ Model Router
→ Checkpoint/State
→ Observability

PostgreSQL is the persistent source of truth, while Redis provides transient infrastructure.

## Consequences

- Subsystems can evolve independently.
- Interfaces become important architectural boundaries.
- The initial implementation can be developed incrementally.
- Distributed-system complexity is introduced only when required by the corresponding milestone.
