package main

import (
	"context"
	"github.com/bft-labs/cometbft-log-etl/internal/app"
	"github.com/bft-labs/cometbft-log-etl/internal/config"
	"github.com/bft-labs/cometbft-log-etl/internal/storage"
	_ "github.com/bft-labs/cometbft-log-etl/ossplugins/block-parts"
	_ "github.com/bft-labs/cometbft-log-etl/ossplugins/consensus-steps"
	_ "github.com/bft-labs/cometbft-log-etl/ossplugins/consensus-timing"
	_ "github.com/bft-labs/cometbft-log-etl/ossplugins/network-latency"
	_ "github.com/bft-labs/cometbft-log-etl/ossplugins/p2p-messages"
	_ "github.com/bft-labs/cometbft-log-etl/ossplugins/timeout-analysis"
	_ "github.com/bft-labs/cometbft-log-etl/ossplugins/tracer-events"
	_ "githu
	_ "github.com/bft-labs/cometbft-log-etl/ossplugins/validator-participation"
	_ "github.com/bft-labs/cometbft-log-etl/ossplugins/tracer-events"
	"github.com/bft-labs/cometbft-log-etl/pkg/pluginloader"
	"github.com/bft-labs/cometbft-log-etl/pkg/pluginsdk"
	"log"
	"os/signal"
	"sort"
	"syscall"
)

func main() {
	// Load configuration
	cfg, err := config.LoadFromFlags()
	if err != nil {
		log.Fatalf("Load config: %v", err)
	}

	// Graceful shutdown context
	base := context.Background()
	ctx, stop := signal.NotifyContext(base, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Setup storage
	mongoStorage, err := storage.NewMongoStorageWithSimulation(ctx, cfg.MongoURI, cfg.SimulationID)
	if err != nil {
		log.Fatalf("Setup storage: %v", err)
	}
	defer mongoStorage.Close(ctx)

	// Fully pluginized path: always use plugins (default processor plugins are auto-enabled).
	srv := app.NewService() // no built-in processors; plugins handle processing

	// Parse events from directory
	entireEvents, err := srv.ParseDirectory(ctx, cfg.Directory)
	if err != nil {
		log.Fatalf("Parse directory: %v", err)
	}
	// Sort events by timestamp
	if len(entireEvents) > 0 {
		sort.Slice(entireEvents, func(i, j int) bool {
			return entireEvents[i].GetTimestamp().Before(entireEvents[j].GetTimestamp())
		})
		storeFunc := mongoStorage.GetEventsStoreFunc(ctx)
		if err := storeFunc(entireEvents); err != nil {
			log.Fatalf("Store events: %v", err)
		}
	}

	// Initialize plugin loader and context
	pctx := pluginsdk.Context{
		Ctx:     ctx,
		Logger:  log.Default(),
		Storage: mongoStorage,
		Metrics: pluginsdk.NoopMetrics{},
		Config:  cfg,
	}
	pl, err := pluginloader.New(ctx, pctx, cfg.Plugins)
	if err != nil {
		log.Fatalf("Init plugins: %v", err)
	}
	defer pl.Finalize()

	// Dispatch events to plugins (core-processors handles processing + persistence)
	for _, evt := range entireEvents {
		pl.Dispatch(evt)
	}
	log.Printf("Successfully processed %d events via plugins", len(entireEvents))
}

// legacy helper removed; we always run via plugins now
