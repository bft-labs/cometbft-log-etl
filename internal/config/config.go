package config

import (
	"flag"
	"fmt"
	"os"

	"github.com/bft-labs/cometbft-log-etl/pkg/pluginsdk"
	"gopkg.in/yaml.v3"
)

// Config holds application configuration.
type Config struct {
	Directory    string
	MongoURI     string
	SimulationID string
	Plugins      []pluginsdk.PluginConfig
}

// GetSimulationID returns the simulation ID (implements pluginsdk.AppConfig).
func (c *Config) GetSimulationID() string { return c.SimulationID }

// LoadFromFlags loads configuration from command line flags.
func LoadFromFlags() (*Config, error) {
	var dirname string
	var mongoURI string
	var simulationID string
	var yamlPath string

	flag.StringVar(&dirname, "dir", "", "Directory path containing JSON logs from multiple nodes")
	flag.StringVar(&mongoURI, "mongo-uri", "mongodb://localhost:27017", "MongoDB connection URI")
	flag.StringVar(&simulationID, "simulation", "", "Simulation ID for database naming")
	flag.StringVar(&yamlPath, "config", "", "Optional YAML config path (plugins, etc.)")
	flag.Parse()

	if dirname == "" {
		return nil, fmt.Errorf("please provide a directory path using the -dir flag")
	}
	if simulationID == "" {
		return nil, fmt.Errorf("please provide a simulation ID using the -simulation flag")
	}

	cfg := &Config{Directory: dirname, MongoURI: mongoURI, SimulationID: simulationID, Plugins: loadPluginsFromYAML(yamlPath)}
	ensureDefaultCoreProcessors(cfg)
	return cfg, nil
}

func loadPluginsFromYAML(path string) []pluginsdk.PluginConfig {
	if path == "" {
		return nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var raw struct {
		Plugins []pluginsdk.PluginConfig `yaml:"plugins"`
	}
	if err := yaml.Unmarshal(b, &raw); err != nil {
		return nil
	}
	return raw.Plugins
}

// ensureDefaultCoreProcessors makes sure the core pipeline exists via plugin
// to keep OSS behavior self-contained without requiring a config file.
func ensureDefaultCoreProcessors(c *Config) {
	if len(c.Plugins) > 0 {
		return
	}
	defaults := []pluginsdk.PluginConfig{
		{Name: "vote-latency", Enabled: true},
		{Name: "block-parts", Enabled: true},
		{Name: "p2p-messages", Enabled: true},
		{Name: "consensus-steps", Enabled: true},
		{Name: "consensus-timing", Enabled: true},
		{Name: "validator-participation", Enabled: true},
		{Name: "network-latency", Enabled: true},
		{Name: "timeout-analysis", Enabled: true},
		{Name: "tracer-events", Enabled: true},
	}
	c.Plugins = append(c.Plugins, defaults...)
}
