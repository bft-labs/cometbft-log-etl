package processor

import (
	"context"
	"fmt"
	"log"

	"github.com/bft-labs/cometbft-analyzer-types/pkg/events"
)

// ProcessorManager manages multiple processors and their results.
type ProcessorManager struct {
	processors []EventProcessor
}

// NewProcessorManager creates a new processor manager.
func NewProcessorManager() *ProcessorManager {
	return &ProcessorManager{
		processors: make([]EventProcessor, 0),
	}
}

// AddProcessor adds a processor to the manager.
func (pm *ProcessorManager) AddProcessor(processor EventProcessor) {
	pm.processors = append(pm.processors, processor)
}

// CreateDefaultProcessors creates the default set of processors.
func (pm *ProcessorManager) CreateDefaultProcessors(ctx context.Context) error {
	// Create vote latency processor
	voteLatencyProc, err := NewProcessor(ctx, ProcessorTypeVoteLatency)
	if err != nil {
		return fmt.Errorf("failed to create vote latency processor: %w", err)
	}
	pm.AddProcessor(voteLatencyProc)

	// Create block parts processor
	blockPartsProc, err := NewProcessor(ctx, ProcessorTypeBlockParts)
	if err != nil {
		return fmt.Errorf("failed to create block parts processor: %w", err)
	}
	pm.AddProcessor(blockPartsProc)

	// Create P2P messages processor
	p2pMessagesProc, err := NewProcessor(ctx, ProcessorTypeP2pMessages)
	if err != nil {
		return fmt.Errorf("failed to create P2P messages processor: %w", err)
	}
	pm.AddProcessor(p2pMessagesProc)

	// Create consensus steps processor
	consensusStepsProc, err := NewProcessor(ctx, ProcessorTypeConsensusSteps)
	if err != nil {
		return fmt.Errorf("failed to create consensus steps processor: %w", err)
	}
	pm.AddProcessor(consensusStepsProc)

	// Create consensus timing processor
	consensusTimingProc, err := NewProcessor(ctx, ProcessorTypeConsensusTiming)
	if err != nil {
		return fmt.Errorf("failed to create consensus timing processor: %w", err)
	}
	pm.AddProcessor(consensusTimingProc)

	// Create validator participation processor
	validatorParticipationProc, err := NewProcessor(ctx, ProcessorTypeValidatorParticipation)
	if err != nil {
		return fmt.Errorf("failed to create validator participation processor: %w", err)
	}
	pm.AddProcessor(validatorParticipationProc)

	// Create network latency processor
	networkLatencyProc, err := NewProcessor(ctx, ProcessorTypeNetworkLatency)
	if err != nil {
		return fmt.Errorf("failed to create network latency processor: %w", err)
	}
	pm.AddProcessor(networkLatencyProc)

	// Create timeout analysis processor
	timeoutAnalysisProc, err := NewProcessor(ctx, ProcessorTypeTimeoutAnalysis)
	if err != nil {
		return fmt.Errorf("failed to create timeout analysis processor: %w", err)
	}
	pm.AddProcessor(timeoutAnalysisProc)

	return nil
}

// ProcessEvents processes events with all processors.
func (pm *ProcessorManager) ProcessEvents(events []events.Event) {
	for _, evt := range events {
		for _, proc := range pm.processors {
			if err := proc.Process(evt); err != nil {
				log.Printf("processor error: %v", err)
			}
		}
	}
}

// CollectResults collects results from all processors that implement ResultsCollector or MultiCollectionResultsCollector.
func (pm *ProcessorManager) CollectResults() []ProcessorResult {
	var results []ProcessorResult

	for _, proc := range pm.processors {
		// First check if it implements MultiCollectionResultsCollector
		if multiCollector, ok := proc.(MultiCollectionResultsCollector); ok {
			multiResults := multiCollector.GetMultiResults()
			results = append(results, multiResults...)
		} else if collector, ok := proc.(ResultsCollector); ok {
			// Fall back to regular ResultsCollector
			data, collectionName := collector.GetResults()
			if len(data) > 0 {
				results = append(results, ProcessorResult{
					Data:           data,
					CollectionName: collectionName,
				})
			}
		}
	}

	return results
}

// GetProcessors returns all processors for use with other services.
func (pm *ProcessorManager) GetProcessors() []EventProcessor {
	return pm.processors
}

// ProcessorResult holds the results from a processor.
type ProcessorResult struct {
	Data           []interface{}
	CollectionName string
}
