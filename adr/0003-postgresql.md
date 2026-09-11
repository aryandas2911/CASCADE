# ADR 0003: PostgreSQL as System of Record

## Status

Accepted

## Context

Cascade requires durable storage for workflows, executions, tasks, checkpoints, events, metrics, and related metadata.

## Decision

PostgreSQL will be Cascade's persistent system of record.

Redis will not be used as the authoritative store for durable workflow state.

## Consequences

- Workflow state receives ACID database guarantees.
- Relational modeling can represent workflow and execution relationships.
- Redis can remain focused on transient infrastructure.
- Database access must eventually be designed around idempotent writes and transaction boundaries.
