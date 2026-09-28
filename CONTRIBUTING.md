# Contributing

Thank you for contributing to the LLM Reliability Control Plane (LRCP). Keep changes narrow, fully tested, and truthful in documentation.

## Development Expectations

- **Durability & Correctness**: Preserve bounded memory, single-writer safety, and explicit acknowledgement semantics (fsync before ACK) on the ingestion path.
- **Dependencies**: Do not add a dependency without documenting its purpose, license, supported platforms, and removal alternative.
- **Performance Integrity**: Do not report an unmeasured performance result as a capability claim.
- **Privacy & Security**: Keep telemetry payload content out of diagnostics unless an explicit content policy permits it.
- **Test Coverage**: Add regression tests in C++ (GTest) and Python (pytest) for every corrected behavior or new feature.

## Verification Checklist Before Submitting PRs

1. **C++ Gateway**:
   ```bash
   cmake -S . -B build -DLRCP_BUILD_TESTS=ON
   cmake --build build
   ctest --test-dir build --output-on-failure
   ```
2. **Python Backend**:
   ```bash
   cd backend
   uv run pytest -v
   ```
3. **Frontend**:
   ```bash
   cd frontend
   npm run build
   ```
4. **Git Hygiene**:
   - Write clear, concise commit messages.
   - Do not include unauthorized attribution tags.
