package main

import (
	"context"
	"github.com/bft-labs/cometbft-analyzer-types/pkg/events"
	"github.com/bft-labs/cometbft-log-etl/pkg/app"
	"github.com/bft-labs/cometbft-log-etl/pkg/config"
	"github.com/bft-labs/cometbft-log-etl/pkg/processor"
	"github.com/bft-labs/cometbft-log-etl/pkg/storage"
	"log"
	"sort"
)

func main() {
	// Load configuration
	cfg, err := config.LoadFromFlags()
	if err != nil {
		log.Fatalf("Load config: %v", err)
	}

	ctx := context.Background()

	// Setup storage
	mongoStorage, err := storage.NewMongoStorageWithSimulation(ctx, cfg.MongoURI, cfg.SimulationID)
	if err != nil {
		log.Fatalf("Setup storage: %v", err)
	}
	defer mongoStorage.Close(ctx)

	// Setup processor manager
	processorManager := processor.NewProcessorManager()
	if err := processorManager.CreateDefaultProcessors(ctx); err != nil {
		log.Fatalf("Create processors: %v", err)
	}

	// Create service
	srv := app.NewService(processorManager.GetProcessors()...)

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

	// Process events
	processorManager.ProcessEvents(entireEvents)

	// Collect processor results and add universal P2P events back to main events stream
	results := processorManager.CollectResults()
	var totalResults int
	var universalP2pEvents []interface{}
	var consensusStepEvents []interface{}

	for _, result := range results {
		// If this is the P2P messages collection, add these events back to main stream
		if result.CollectionName == "p2p_messages" {
			universalP2pEvents = append(universalP2pEvents, result.Data...)
			log.Printf("Adding %d universal P2P events back to main events stream", len(result.Data))
		}

		// If this is the consensus steps collection, collect these for frontend
		if result.CollectionName == "consensus_steps" {
			consensusStepEvents = append(consensusStepEvents, result.Data...)
			log.Printf("Collected %d consensus step events for frontend", len(result.Data))
		}

		if err := mongoStorage.StoreResults(ctx, result.Data, result.CollectionName); err != nil {
			log.Printf("Store results error for %s: %v", result.CollectionName, err)
		} else {
			totalResults += len(result.Data)
			log.Printf("Stored %d results in %s collection", len(result.Data), result.CollectionName)
		}
	}

	// Create frontend collection combining consensus steps with important P2P messages
	frontendEvents := append(consensusStepEvents, universalP2pEvents...)
	// Sort frontend events by timestamp
	sort.Slice(frontendEvents, func(i, j int) bool {
		// Change both i and j to events.Event to access GetTimestamp method
		frontendEventI, okI := frontendEvents[i].(events.Event)
		frontendEventJ, okJ := frontendEvents[j].(events.Event)
		if !okI || !okJ {
			log.Printf("Skipping sorting for non-event types: %T and %T", frontendEvents[i], frontendEvents[j])
			return false
		}
		return frontendEventI.GetTimestamp().Before(frontendEventJ.GetTimestamp())
	})
	if len(frontendEvents) > 0 {
		if err := mongoStorage.StoreResults(ctx, frontendEvents, "consensus_events"); err != nil {
			log.Printf("Store frontend consensus events error: %v", err)
		} else {
			log.Printf("Stored %d events in frontend_consensus collection (consensus: %d, p2p: %d)",
				len(frontendEvents), len(consensusStepEvents), len(universalP2pEvents))
		}
	}

	log.Printf("Successfully processed %d events and stored %d total results", len(entireEvents), totalResults)
}
