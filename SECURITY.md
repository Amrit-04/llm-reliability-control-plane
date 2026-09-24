# Security policy

## Current scope

M1 treats OTLP requests as untrusted input. It applies an HTTP body limit and protobuf parsing, but it does not yet authenticate clients, isolate tenants, encrypt transport, persist telemetry, or perform PII redaction.

Do not expose the M1 gateway directly to an untrusted network. Put it behind authenticated TLS infrastructure during development if remote clients are necessary.

## Reporting a vulnerability

Until a project security contact is configured, do not disclose sensitive findings in a public issue. Contact the repository maintainer privately and include reproduction steps, affected revision, and potential impact.
