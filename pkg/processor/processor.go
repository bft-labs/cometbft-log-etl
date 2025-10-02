package processor

import (
	"context"
	"fmt"
)

// ProcessorType represents the type of processor to create.
type ProcessorType string

const (
	ProcessorTypeVoteLatency            ProcessorType = "vote_latency"
	ProcessorTypeBlockParts             ProcessorType = "block_parts"
	ProcessorTypeP2pMessages            ProcessorType = "p2p_messages"
	ProcessorTypeConsensusSteps         ProcessorType = "consensus_steps"
	ProcessorTypeConsensusTiming        ProcessorType = "consensus_timing"
	ProcessorTypeValidatorParticipation ProcessorType = "validator_participation"
	ProcessorTypeNetworkLatency         ProcessorType = "network_latency"
	ProcessorTypeTimeoutAnalysis        ProcessorType = "timeout_analysis"
)

// NewProcessor returns a processor based on the specified type.
func NewProcessor(ctx context.Context, processorType ProcessorType) (EventProcessor, error) {
	switch processorType {
	case ProcessorTypeVoteLatency:
		return NewVoteLatencyProcessor(ctx), nil
	case ProcessorTypeBlockParts:
		return NewBlockPartsProcessor(ctx), nil
	case ProcessorTypeP2pMessages:
		return NewP2pMessageProcessor(ctx), nil
	case ProcessorTypeConsensusSteps:
		return NewConsensusStepProcessor(ctx), nil
	case ProcessorTypeConsensusTiming:
		return NewConsensusTimingProcessor(ctx), nil
	case ProcessorTypeValidatorParticipation:
		return NewValidatorParticipationProcessor(ctx), nil
	case ProcessorTypeNetworkLatency:
		return NewNetworkLatencyProcessor(ctx), nil
	case ProcessorTypeTimeoutAnalysis:
		return NewTimeoutAnalysisProcessor(ctx), nil
	default:
		return nil, fmt.Errorf("unknown processor type: %s", processorType)
	}
}
