# cometbft-log-etl

Open-source ETL for CometBFT logs. Drop in log files, get a normalized database with core metrics. A lightweight plugin system enables advanced analyses as separate OSS or premium plugins without changing the core.

## Quickstart

- Requirements: Go 1.21+ and a local MongoDB.
- Example logs are included under `example-logs/`.

Run with defaults (example logs + demo simulation id):

```bash
make run            # uses DIR=example-logs/normal SIM=demo-sim
```

Or run directly:

```bash
go run . -dir example-logs/normal -simulation demo-sim
```

Flags:

- `-dir`: Directory containing CometBFT `.log` files (required)
- `-simulation`: Simulation ID for DB naming (required)
- `-mongo-uri`: MongoDB URI (default: `mongodb://localhost:27017`)
- `-config`: Optional YAML config path (plugins enablement)

## What It Does

- Parses log lines into typed raw events
- Converts raw events into normalized events
- Stores events and computed results in MongoDB
- All processing is plugin-driven. Each built-in processor ships as a separate OSS plugin so contributors can add/replace processors easily.
- Dispatches each event to enabled plugins via the plugin SDK.

## Plugin System

Plugins are dynamically registered and enabled via config. Core provides only the SDK, loader, and OSS processor plugins.

Config example (`config.yaml`):

```yaml
plugins:
  - name: "vote-latency"
    enabled: true
  - name: "block-parts"
    enabled: true
  - name: "p2p-messages"
    enabled: true
  - name: "consensus-steps"
    enabled: true
  - name: "consensus-timing"
    enabled: true
  - name: "validator-participation"
    enabled: true
  - name: "network-latency"
    enabled: true
  - name: "timeout-analysis"
    enabled: true
  - name: "peer-participation"  # example placeholder; not included
    enabled: false
  - name: "anomaly-detection"   # Premium-only, ignored in OSS unless provided
    enabled: false
```

Run with config:

```bash
go run . -dir example-logs/normal -simulation demo-sim -config config.yaml
```

Environment flag for premium:

- `CV_PRO_ENABLED=true` – if set but named premium plugins aren’t present, a warning is logged and the app runs normally. Note: if no config is provided, a default set of core processor plugins is auto-enabled so the pipeline works out of the box.

### OSS vs Premium

- OSS includes:
  - Core ETL: parsing, normalization, DB storage (Mongo)
  - Plugin SDK (`pkg/pluginsdk`): interfaces and context
  - Plugin Loader (`pkg/pluginloader`): registry + lifecycle
  - Example OSS plugin(s) under `ossplugins/`
- Premium/Custom:
  - Never included in this repo
  - Built in private repos; register via `pluginloader.Register()` in `init()`
  - Referenced at build time in private distributions

## Project Structure

- `internal/parser`, `internal/converter`, `internal/storage`, `internal/app`, `internal/config` – core ETL code used by the binary
- `pkg/pluginsdk`, `pkg/pluginloader` – public SDK + loader for OSS/premium/custom plugins
- `pkg/pluginsdk` – Plugin interfaces and Context
- `pkg/pluginloader` – Loader, registry, lifecycle, dispatching
- `ossplugins/` – Open-source plugins (each processor owns its logic under its plugin directory)
- `premiumplugins/` – README + stubs only (no code)

## Plugin SDK (Go)

Interface:

```go
type Plugin interface {
    Name() string
    Init(ctx Context) error
    Process(event types.RawEvent) error
    Finalize() error
}
```

Context gives access to logger, storage, metrics, and app config:

```go
type Context struct {
    Ctx     context.Context
    Logger  Logger
    Storage Storage
    Metrics Metrics
    Config  AppConfig
}
```

Storage interface (backed by Mongo in OSS):

```go
type Storage interface {
    StoreResults(ctx context.Context, results []interface{}, collectionName string) error
}
```

## Example OSS Plugins

`ossplugins/processors` – registers one plugin per built-in processor (vote-latency, block-parts, p2p-messages, consensus-steps, consensus-timing, validator-participation, network-latency, timeout-analysis).

## Writing Your Own Plugin

1) Add a new package (OSS) under `ossplugins/<your-plugin>` in this repo, or create a premium plugin in a private repo.
2) Implement `pluginsdk.Plugin` and call `pluginloader.Register("your-name", factory)` in `init()`.
3) Enable it in `config.yaml`.

HelloWorld example:

```go
type HelloWorld struct { ctx pluginsdk.Context }
func (p *HelloWorld) Name() string { return "hello-world" }
func (p *HelloWorld) Init(ctx pluginsdk.Context) error { p.ctx = ctx; return nil }
func (p *HelloWorld) Process(event interface{}) error { p.ctx.Logger.Printf("saw: %T", event); return nil }
func (p *HelloWorld) Finalize() error { return nil }
func init() { pluginloader.Register("hello-world", func() pluginsdk.Plugin { return &HelloWorld{} }) }
```

## Tests

- Plugin lifecycle test at `pkg/pluginloader/loader_test.go` verifies init/process/finalize and storage calls.
- Example logs under `example-logs/` support quick manual runs.

## Contributing

- For core changes: keep updates minimal and focused. Avoid coupling core to plugins.
- For plugins: prefer storing results via the provided `Storage` interface. Avoid importing internals beyond the SDK.
- PRs adding more OSS plugins and hooks are welcome.
