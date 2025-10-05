package pluginsdk

import (
	"context"
	appTypes "github.com/bft-labs/cometbft-log-etl/types"
)

// Plugin defines the lifecycle and processing interface for plugins.
// Plugins should be stateless where possible and perform heavier work in Finalize.
type Plugin interface {
	// Name returns a unique, human-readable name for the plugin.
	Name() string
	// Init provides the plugin with context and allows it to prepare resources.
	Init(ctx Context) error
	// Process handles a single event from the ETL pipeline.
	Process(event appTypes.RawEvent) error
	// Finalize is called on shutdown to flush and persist results.
	Finalize() error
}

// Storage exposes a minimal API for plugins to persist results.
// OSS plugins should prefer storing to the main database via this interface.
type Storage interface {
	StoreResults(ctx context.Context, results []interface{}, collectionName string) error
}

// Metrics exposes a minimal API for plugins to emit counters/gauges if desired.
// Implementations may be a no-op in the OSS distribution.
type Metrics interface {
	IncCounter(name string, labels map[string]string)
	ObserveHistogram(name string, value float64, labels map[string]string)
}

// NoopMetrics is a default metrics implementation doing nothing.
type NoopMetrics struct{}

func (NoopMetrics) IncCounter(_ string, _ map[string]string)                  {}
func (NoopMetrics) ObserveHistogram(_ string, _ float64, _ map[string]string) {}

// Context gives plugins access to common facilities without coupling to internals.
type Context struct {
	// Ctx is a base context for background operations.
	Ctx context.Context
	// Logger is a standard logger; plugins should use it for diagnostics.
	Logger Logger
	// Storage allows persisting plugin results to the main DB.
	Storage Storage
	// Metrics for optional instrumentation.
	Metrics Metrics
	// Config is the loaded application config.
	Config AppConfig
}

// Logger is the subset of log.Logger used by plugins.
type Logger interface {
	Printf(format string, v ...interface{})
}

// AppConfig is a read-only view of app configuration relevant to plugins.
type AppConfig interface {
	GetSimulationID() string
}

// PluginConfig declares plugin selection in app config. Kept in SDK so
// external tools can share the structure without depending on internal packages.
type PluginConfig struct {
	Name    string `yaml:"name"`
	Enabled bool   `yaml:"enabled"`
}
