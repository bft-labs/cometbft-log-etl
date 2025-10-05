package consensussteps

import (
	"context"

	"github.com/bft-labs/cometbft-analyzer-types/pkg/events"
)

// Processor collects consensus-related events excluding P2P send/receive messages.
type Processor struct {
	ctx context.Context
	// Collected consensus events
	consensusEvents []interface{}
}

func NewProcessor(ctx context.Context) *Processor {
	return &Processor{ctx: ctx, consensusEvents: make([]interface{}, 0)}
}

// Process filters only consensus-related events, excluding P2P send/receive.
func (p *Processor) Process(evt events.Event) error {
	switch evt.(type) {
	case *events.EventEnteringNewRound,
		*events.EventEnteringPrevoteStep,
		*events.EventEnteringPrevoteWaitStep,
		*events.EventEnteringPrecommitStep,
		*events.EventEnteringPrecommitWaitStep,
		*events.EventEnteringCommitStep,
		*events.EventCommittedBlock,
		*events.EventProposeStep,
		*events.EventReceivedProposal,
		*events.EventReceivedCompleteProposalBlock,
		*events.EventScheduledTimeout:
		p.consensusEvents = append(p.consensusEvents, evt)
		return nil
	// Skip P2P low-level messages; handled by P2P plugin
	case *events.EventSendVote,
		*events.EventReceivePacketVote,
		*events.EventSendNewRoundStep,
		*events.EventReceivePacketNewRoundStep,
		*events.EventSendNewValidBlock,
		*events.EventReceivePacketNewValidBlock,
		*events.EventSendProposal,
		*events.EventReceivePacketProposal,
		*events.EventSendProposalPOL,
		*events.EventReceivePacketProposalPOL,
		*events.EventSendBlockPart,
		*events.EventReceivePacketBlockPart,
		*events.EventSendHasVote,
		*events.EventReceivePacketHasVote,
		*events.EventSendVoteSetMaj23,
		*events.EventReceivePacketVoteSetMaj23,
		*events.EventSendVoteSetBits,
		*events.EventReceivePacketVoteSetBits,
		*events.EventSendHasProposalBlockPart,
		*events.EventReceivePacketHasProposalBlockPart:
		return nil
	default:
		return nil
	}
}

func (p *Processor) GetResults() ([]interface{}, string) {
	return p.consensusEvents, "consensus_steps"
}
