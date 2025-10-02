package processor

import (
	"context"
	"fmt"
	"github.com/bft-labs/cometbft-analyzer-types/pkg/keys"

	"github.com/bft-labs/cometbft-analyzer-types/pkg/events"
	v "github.com/bft-labs/cometbft-analyzer-types/pkg/statistics/vote"
)

// VoteLatencyProcessor collects vote message latency statistics.
type VoteLatencyProcessor struct {
	ctx context.Context

	vlm          map[string]*v.VoteLatency
	confirmedVls []*v.VoteLatency
}

// NewVoteLatencyProcessor returns a VoteLatencyProcessor.
func NewVoteLatencyProcessor(ctx context.Context) *VoteLatencyProcessor {
	return &VoteLatencyProcessor{
		ctx:          ctx,
		vlm:          make(map[string]*v.VoteLatency),
		confirmedVls: make([]*v.VoteLatency, 0),
	}
}

// Process implements EventProcessor.
func (p *VoteLatencyProcessor) Process(evt events.Event) error {
	switch e := evt.(type) {
	case *events.EventReceivePacketVote:
		vote := e.Vote
		voteKey, err := voteKeyFromEvent(e)
		if err != nil {
			return fmt.Errorf("failed to get v key from event: %w", err)
		}
		hash := voteKey.Hash()
		vl := p.vlm[hash]
		if vl == nil {
			// This should not happen in normal operation, but we handle it gracefully.
			vl = &v.VoteLatency{
				Status:          v.VoteMsgStatusReceived,
				Vote:            vote,
				SenderPeerId:    voteKey.Sender,
				RecipientPeerId: voteKey.Receiver,
				ReceivedTime:    e.Timestamp,
			}
		} else {
			vl.Status = v.VoteMsgStatusConfirmed
			vl.ReceivedTime = e.Timestamp
			vl.ConfirmedTime = e.Timestamp
			vl.Latency = e.Timestamp.Sub(vl.SentTime)
			p.confirmedVls = append(p.confirmedVls, vl)
		}
		p.vlm[hash] = vl
	case *events.EventSendVote:
		vote := e.Vote
		voteKey, err := voteKeyFromEvent(e)
		if err != nil {
			return fmt.Errorf("failed to get v key from event: %w", err)
		}
		vl := &v.VoteLatency{
			Status:          v.VoteMsgStatusSent,
			Vote:            vote,
			SenderPeerId:    voteKey.Sender,
			RecipientPeerId: voteKey.Receiver,
			SentTime:        e.Timestamp,
		}
		p.vlm[voteKey.Hash()] = vl
	}
	return nil
}

func voteKeyFromEvent(evt events.Event) (*keys.VoteKey, error) {
	if e, ok := evt.(*events.EventReceivePacketVote); ok {
		return &keys.VoteKey{
			Height:   e.Vote.Height,
			Round:    e.Vote.Round,
			ValIdx:   e.Vote.ValidatorIndex,
			Sender:   e.SourcePeerId,
			Receiver: e.NodeId,
		}, nil
	} else if e, ok := evt.(*events.EventSendVote); ok {
		return &keys.VoteKey{
			Height:   e.Vote.Height,
			Round:    e.Vote.Round,
			ValIdx:   e.Vote.ValidatorIndex,
			Sender:   e.NodeId,
			Receiver: e.RecipientPeerId,
		}, nil
	}
	return nil, fmt.Errorf("unsupported event type: %T", evt)
}

// GetConfirmedVoteLatencies returns the confirmed vote latencies collected so far.
func (v *VoteLatencyProcessor) GetConfirmedVoteLatencies() []*v.VoteLatency {
	return v.confirmedVls
}

// GetResults implements ResultsCollector interface.
func (p *VoteLatencyProcessor) GetResults() ([]interface{}, string) {
	results := make([]interface{}, len(p.confirmedVls))
	for i, vl := range p.confirmedVls {
		results[i] = vl
	}
	return results, "vote_latencies"
}
