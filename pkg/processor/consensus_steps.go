package processor

import (
	"context"

	"github.com/bft-labs/cometbft-analyzer-types/pkg/events"
)

// ConsensusStepProcessor collects consensus-related events excluding P2P send/receive messages.
// This provides a clean view of consensus steps and important consensus actions.
type ConsensusStepProcessor struct {
	ctx context.Context

	// Collected consensus events
	consensusEvents []interface{}
}

// NewConsensusStepProcessor returns a ConsensusStepProcessor.
func NewConsensusStepProcessor(ctx context.Context) *ConsensusStepProcessor {
	return &ConsensusStepProcessor{
		ctx:             ctx,
		consensusEvents: make([]interface{}, 0),
	}
}

// Process implements EventProcessor.
func (p *ConsensusStepProcessor) Process(evt events.Event) error {
	// Filter only consensus-related events, excluding P2P send/receive
	switch evt.(type) {
	// Consensus step transitions
	case *events.EventEnteringNewRound:
		p.consensusEvents = append(p.consensusEvents, evt)
	case *events.EventEnteringPrevoteStep:
		p.consensusEvents = append(p.consensusEvents, evt)
	case *events.EventEnteringPrevoteWaitStep:
		p.consensusEvents = append(p.consensusEvents, evt)
	case *events.EventEnteringPrecommitStep:
		p.consensusEvents = append(p.consensusEvents, evt)
	case *events.EventEnteringPrecommitWaitStep:
		p.consensusEvents = append(p.consensusEvents, evt)
	case *events.EventEnteringCommitStep:
		p.consensusEvents = append(p.consensusEvents, evt)
	case *events.EventCommittedBlock:
		p.consensusEvents = append(p.consensusEvents, evt)
	case *events.EventProposeStep:
		p.consensusEvents = append(p.consensusEvents, evt)

	// Important consensus events
	case *events.EventReceivedProposal:
		p.consensusEvents = append(p.consensusEvents, evt)
	case *events.EventReceivedCompleteProposalBlock:
		p.consensusEvents = append(p.consensusEvents, evt)
	case *events.EventScheduledTimeout:
		p.consensusEvents = append(p.consensusEvents, evt)

	// Vote-related consensus events (but not the raw P2P send/receive)
	// These are higher-level consensus actions rather than P2P messages

	// Skip P2P send/receive events - these are handled by P2P processor
	case *events.EventSendVote:
		return nil
	case *events.EventReceivePacketVote:
		return nil
	case *events.EventSendNewRoundStep:
		return nil
	case *events.EventReceivePacketNewRoundStep:
		return nil
	case *events.EventSendNewValidBlock:
		return nil
	case *events.EventReceivePacketNewValidBlock:
		return nil
	case *events.EventSendProposal:
		return nil
	case *events.EventReceivePacketProposal:
		return nil
	case *events.EventSendProposalPOL:
		return nil
	case *events.EventReceivePacketProposalPOL:
		return nil
	case *events.EventSendBlockPart:
		return nil
	case *events.EventReceivePacketBlockPart:
		return nil
	case *events.EventSendHasVote:
		return nil
	case *events.EventReceivePacketHasVote:
		return nil
	case *events.EventSendVoteSetMaj23:
		return nil
	case *events.EventReceivePacketVoteSetMaj23:
		return nil
	case *events.EventSendVoteSetBits:
		return nil
	case *events.EventReceivePacketVoteSetBits:
		return nil
	case *events.EventSendHasProposalBlockPart:
		return nil
	case *events.EventReceivePacketHasProposalBlockPart:
		return nil

	// Include any other consensus-related events that might be added in the future
	default:
		// For unknown events, we skip them to keep the collection clean
		return nil
	}

	return nil
}

// GetResults implements ResultsCollector interface.
func (p *ConsensusStepProcessor) GetResults() ([]interface{}, string) {
	return p.consensusEvents, "consensus_steps"
}
