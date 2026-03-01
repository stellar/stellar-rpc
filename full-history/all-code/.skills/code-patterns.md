# Code Patterns

## Dependencies injected via Config struct — never global

```go
type Config struct {
    Store  interfaces.TxHashStore
    Logger logging.Logger
    Memory memory.Monitor
}

type Builder struct {
    store  interfaces.TxHashStore
    log    logging.Logger
    memory memory.Monitor
}

func New(cfg Config) *Builder {
    if cfg.Logger == nil { panic("Builder: Logger required") }
    return &Builder{
        store:  cfg.Store,
        log:    cfg.Logger.WithScope("BUILDER"),
        memory: cfg.Memory,
    }
}
```

## Receiver methods for all behavior

Free functions only for constructors (`New`) and pure utilities in `pkg/`.

```go
func (b *Builder) Run() (*Stats, error) { ... }      // CORRECT
func RunBuilder(b *Builder) (*Stats, error) { ... }   // WRONG
```

## Code to interfaces (pkg/interfaces/), not concrete types

```go
func (o *Orchestrator) ingest(store interfaces.TxHashStore) error { ... }  // CORRECT
func (o *Orchestrator) ingest(store *store.RocksDBStore) error { ... }     // WRONG
```

## Scoped loggers

Every component calls `cfg.Logger.WithScope("NAME")` in its constructor. Scopes nest: `[BACKFILL:INGEST]`.

## Logger interface (from pkg/logging/)

```go
type Logger interface {
    Info(format string, args ...interface{})
    Error(format string, args ...interface{})
    Separator()
    Sync()
    Close()
    WithScope(scope string) Logger
}
```

## Every component follows this shape

1. `Config` struct — all deps as interfaces
2. Private struct — unexported fields
3. `New(cfg Config)` — validates required deps, creates scoped logger
4. `Run() (*Stats, error)` — entry point
5. `Stats` struct — counts, bytes, durations, `*stats.LatencyStats` (from `pkg/stats/`)
