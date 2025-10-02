package processor

import (
	"context"
	"fmt"
	"github.com/bft-labs/cometbft-analyzer-types/pkg/keys"
	"time"

	"github.com/bft-labs/cometbft-analyzer-types/pkg/core"
	"github.com/bft-labs/cometbft-analyzer-types/pkg/events"
)

// BlockPartStatus represents the status of a block part message.
type BlockPartStatus string

const (
	BlockPartStatusSent      BlockPartStatus = "sent"
	BlockPartStatusReceived  BlockPartStatus = "received"
	BlockPartStatusConfirmed BlockPartStatus = "confirmed"
)

// BlockPartLatency represents latency statistics for a block part message.
type BlockPartLatency struct {
	Status          BlockPartStatus
	Part            core.Part
	Height          int64
	Round           int32
	SenderPeerId    string
	RecipientPeerId string
	SentTime        time.Time
	ReceivedTime    time.Time
	ConfirmedTime   time.Time
	Latency         time.Duration
}

// BlockPartsProcessor collects block part message latency statistics.
type BlockPartsProcessor struct {
	ctx context.Context

	bplm          map[string]*BlockPartLatency
	confirmedBpls []*BlockPartLatency
}

// NewBlockPartsProcessor returns a BlockPartsProcessor.
func NewBlockPartsProcessor(ctx context.Context) *BlockPartsProcessor {
	return &BlockPartsProcessor{
		ctx:           ctx,
		bplm:          make(map[string]*BlockPartLatency),
		confirmedBpls: make([]*BlockPartLatency, 0),
	}
}

// Process implements EventProcessor.
func (p *BlockPartsProcessor) Process(evt events.Event) error {
	switch e := evt.(type) {
	case *events.EventReceivePacketBlockPart:
		part := e.Part
		blockPartKey, err := blockPartKeyFromEvent(e)
		if err != nil {
			return fmt.Errorf("failed to get block part key from event: %w", err)
		}
		hash := blockPartKey.Hash()
		bpl := p.bplm[hash]
		if bpl == nil {
			// This should not happen in normal operation, but we handle it gracefully.
			bpl = &BlockPartLatency{
				Status:          BlockPartStatusReceived,
				Part:            part,
				Height:          e.Height,
				Round:           e.Round,
				SenderPeerId:    blockPartKey.Sender,
				RecipientPeerId: blockPartKey.Receiver,
				ReceivedTime:    e.Timestamp,
			}
		} else {
			bpl.Status = BlockPartStatusConfirmed
			bpl.ReceivedTime = e.Timestamp
			bpl.ConfirmedTime = e.Timestamp
			bpl.Latency = e.Timestamp.Sub(bpl.SentTime)
			p.confirmedBpls = append(p.confirmedBpls, bpl)
		}
		p.bplm[hash] = bpl
	case *events.EventSendBlockPart:
		part := e.Part
		blockPartKey, err := blockPartKeyFromEvent(e)
		if err != nil {
			return fmt.Errorf("failed to get block part key from event: %w", err)
		}
		bpl := &BlockPartLatency{
			Status:          BlockPartStatusSent,
			Part:            part,
			Height:          e.Height,
			Round:           e.Round,
			SenderPeerId:    blockPartKey.Sender,
			RecipientPeerId: blockPartKey.Receiver,
			SentTime:        e.Timestamp,
		}
		p.bplm[blockPartKey.Hash()] = bpl
	}
	return nil
}

func blockPartKeyFromEvent(evt events.Event) (*keys.BlockPartKey, error) {
	if e, ok := evt.(*events.EventReceivePacketBlockPart); ok {
		return &keys.BlockPartKey{
			Height:   e.Height,
			Round:    e.Round,
			Index:    e.Part.Index,
			Sender:   e.SourcePeerId,
			Receiver: e.NodeId,
		}, nil
	} else if e, ok := evt.(*events.EventSendBlockPart); ok {
		return &keys.BlockPartKey{
			Height:   e.Height,
			Round:    e.Round,
			Index:    e.Part.Index,
			Sender:   e.NodeId,
			Receiver: e.RecipientPeerId,
		}, nil
	}
	return nil, fmt.Errorf("unsupported event type: %T", evt)
}

// GetConfirmedBlockPartLatencies returns the confirmed block part latencies collected so far.
func (bp *BlockPartsProcessor) GetConfirmedBlockPartLatencies() []*BlockPartLatency {
	return bp.confirmedBpls
}

// GetResults implements ResultsCollector interface.
func (bp *BlockPartsProcessor) GetResults() ([]interface{}, string) {
	results := make([]interface{}, len(bp.confirmedBpls))
	for i, bpl := range bp.confirmedBpls {
		results[i] = bpl
	}
	return results, "block_part_latencies"
}
