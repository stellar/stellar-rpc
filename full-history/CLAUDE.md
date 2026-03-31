# Full History

Project conventions and reference docs are in Claude project memory (ref_*.md files).
Read the relevant memory files before writing code:

- `ref_code_patterns.md` — Go style: interfaces, receivers, DI, scoped loggers
- `ref_metrics_and_logging.md` — Mandatory metrics, 1-min progress, percentiles, final summaries
- `ref_pkg_api.md` — All pkg/ sub-packages with signatures. Never duplicate.
- `ref_architecture.md` — Range/chunk math, meta store keys, state machines, crash recovery
- `ref_build_and_test.md` — CGO setup, Makefile targets, testing rules

## Directory layout

```
full-history/
├── backfill/        ← backfill pipeline (package backfill)
│   └── cmd/main.go  ← binary entry point
├── pkg/             ← shared utility packages
├── Makefile         ← build targets (make build, make clean)
├── design-docs/     ← current design documents
└── design-docs-og/  ← original design documents
```

## Build artifacts

After compiling binaries (e.g. `make build`), always remove `bin/` before finishing. Binaries must never be committed — `bin/` is gitignored.
