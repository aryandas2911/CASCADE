# ADR 0002: Redis Streams for Initial Task Queue

## Status

Accepted

## Context

Cascade requires asynchronous communication between scheduling and worker components.

The initial system needs a persistent queue with consumer-group support that is practical for local development.

## Decision

Redis Streams will be used as the initial task queue and event transport.

## Consequences

- Local development remains relatively simple.
- Consumer groups provide a foundation for distributed workers.
- Redis remains transient infrastructure rather than the system of record.
- PostgreSQL remains responsible for durable workflow state.
- A different queue technology can be evaluated later if measured requirements justify it.
