# Contributing

This project is at an early milestone. Keep changes narrow, tested, and truthful in documentation.

## Development expectations

- Do not add a dependency without documenting its purpose, license, supported platforms, and removal alternative.
- Do not report an unmeasured performance result as a capability claim.
- Keep telemetry payload content out of diagnostics unless an explicit content policy permits it.
- Add a regression test for every corrected receiver behavior.
- Preserve bounded memory and explicit acknowledgement semantics on the ingestion path.

Before opening a change, configure, build, and run CTest using the commands in the README. Run sanitizers where supported by the compiler.
