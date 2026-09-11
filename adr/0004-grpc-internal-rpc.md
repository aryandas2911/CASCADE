# ADR 0004: gRPC for Internal RPC

## Status

Accepted

## Context

Cascade contains multiple logical services that need typed internal communication.

## Decision

Cascade will use gRPC with Protocol Buffers for internal service-to-service communication.

REST will be used for client-facing APIs.

## Consequences

- Internal service contracts are strongly typed.
- Protocol Buffers provide explicit message definitions.
- Streaming communication is supported.
- Internal communication remains distinct from the public REST API.
