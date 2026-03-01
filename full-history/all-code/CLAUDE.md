# All-Code

Before writing any code in this directory, read the relevant skill files in `.skills/`:

- `.skills/code-patterns.md` — Go style: interfaces, receivers, DI, scoped loggers
- `.skills/metrics-and-logging.md` — Mandatory metrics, 1-min progress, percentiles, final summaries
- `.skills/helpers-ref.md` — All helper functions with signatures. Never duplicate.
- `.skills/architecture.md` — Range/chunk math, meta store keys, state machines, crash recovery
- `.skills/build-and-test.md` — CGO setup, Makefile targets, testing rules

## Build artifacts

After compiling binaries (e.g. `make build`), always remove `bin/` before finishing. Binaries must never be committed — `bin/` is gitignored.
