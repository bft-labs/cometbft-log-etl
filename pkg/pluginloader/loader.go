package pluginloader

import (
	"context"
	"fmt"
	"os"

	"github.com/bft-labs/cometbft-log-etl/pkg/pluginsdk"
	appTypes "github.com/bft-labs/cometbft-log-etl/types"
)

// Factory creates a new Plugin instance.
type Factory func() pluginsdk.Plugin

var registry = map[string]Factory{}

// Register allows plugins (OSS or premium) to self-register by name.
func Register(name string, f Factory) {
	registry[name] = f
}

// Loader is responsible for managing plugin lifecycle and dispatching events.
type Loader struct {
	plugins []pluginsdk.Plugin
	ctx     pluginsdk.Context
	logger  pluginsdk.Logger
}

// New creates a Loader and initializes enabled plugins from config.
func New(baseCtx context.Context, pctx pluginsdk.Context, cfgs []pluginsdk.PluginConfig) (*Loader, error) {
	l := &Loader{ctx: pctx, logger: pctx.Logger}
	proEnabled := os.Getenv("CV_PRO_ENABLED") == "true"

	for _, pcfg := range cfgs {
		if !pcfg.Enabled {
			continue
		}
		factory, ok := registry[pcfg.Name]
		if !ok {
			if proEnabled {
				pctx.Logger.Printf("[pluginloader] premium/custom plugin '%s' not found; continuing without it", pcfg.Name)
			} else {
				pctx.Logger.Printf("[pluginloader] plugin '%s' not found in OSS registry; skipping", pcfg.Name)
			}
			continue
		}
		inst := factory()
		if err := inst.Init(pctx); err != nil {
			return nil, fmt.Errorf("init plugin %s: %w", pcfg.Name, err)
		}
		l.plugins = append(l.plugins, inst)
		pctx.Logger.Printf("[pluginloader] initialized plugin: %s", inst.Name())
	}
	_ = baseCtx // reserved for future cancellation control
	return l, nil
}

// Dispatch passes a single event to all active plugins.
func (l *Loader) Dispatch(evt appTypes.RawEvent) {
	for _, p := range l.plugins {
		if err := p.Process(evt); err != nil {
			l.logger.Printf("[pluginloader] plugin %s process error: %v", p.Name(), err)
		}
	}
}

// Finalize calls Finalize on all active plugins.
func (l *Loader) Finalize() {
	for _, p := range l.plugins {
		if err := p.Finalize(); err != nil {
			l.logger.Printf("[pluginloader] plugin %s finalize error: %v", p.Name(), err)
		}
	}
}
