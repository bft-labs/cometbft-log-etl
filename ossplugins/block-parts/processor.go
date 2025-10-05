package blockparts

import (
	"context"
	"fmt"
	"github.com/bft-labs/cometbft-analyzer-types/pkg/core"
	"github.com/bft-labs/cometbft-analyzer-types/pkg/events"
	"github.com/bft-labs/cometbft-analyzer-types/pkg/keys"
	"time"
)

type Status string

const (
	StatusSent      Status = "sent"
	StatusReceived  Status = "received"
	StatusConfirmed Status = "confirmed"
)

type Latency struct {
	Status          Status
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

type Processor struct {
	ctx           context.Context
	bplm          map[string]*Latency
	confirmedBpls []*Latency
}

func NewProcessor(ctx context.Context) *Processor {
	return &Processor{ctx: ctx, bplm: make(map[string]*Latency), confirmedBpls: make([]*Latency, 0)}
}

func (p *Processor) Process(evt events.Event) error {
	switch e := evt.(type) {
	case *events.EventReceivePacketBlockPart:
		part := e.Part
		key, err := keyFromEvent(e)
		if err != nil {
			return fmt.Errorf("failed to get block part key from event: %w", err)
		}
		hash := key.Hash()
		bpl := p.bplm[hash]
		if bpl == nil {
			bpl = &Latency{Status: StatusReceived, Part: part, Height: e.Height, Round: e.Round, SenderPeerId: key.Sender, RecipientPeerId: key.Receiver, ReceivedTime: e.Timestamp}
		} else {
			bpl.Status = StatusConfirmed
			bpl.ReceivedTime = e.Timestamp
			bpl.ConfirmedTime = e.Timestamp
			bpl.Latency = e.Timestamp.Sub(bpl.SentTime)
			p.confirmedBpls = append(p.confirmedBpls, bpl)
		}
		p.bplm[hash] = bpl
	case *events.EventSendBlockPart:
		part := e.Part
		key, err := keyFromEvent(e)
		if err != nil {
			return fmt.Errorf("failed to get block part key from event: %w", err)
		}
		bpl := &Latency{Status: StatusSent, Part: part, Height: e.Height, Round: e.Round, SenderPeerId: key.Sender, RecipientPeerId: key.Receiver, SentTime: e.Timestamp}
		p.bplm[key.Hash()] = bpl
	}
	return nil
}

func keyFromEvent(evt events.Event) (*keys.BlockPartKey, error) {
	if e, ok := evt.(*events.EventReceivePacketBlockPart); ok {
		return &keys.BlockPartKey{Height: e.Height, Round: e.Round, Index: e.Part.Index, Sender: e.SourcePeerId, Receiver: e.NodeId}, nil
	} else if e, ok := evt.(*events.EventSendBlockPart); ok {
		return &keys.BlockPartKey{Height: e.Height, Round: e.Round, Index: e.Part.Index, Sender: e.NodeId, Receiver: e.RecipientPeerId}, nil
	}
	return nil, fmt.Errorf("unsupported event type: %T", evt)
}

func (p *Processor) GetResults() ([]interface{}, string) {
	results := make([]interface{}, len(p.confirmedBpls))
	for i, r := range p.confirmedBpls {
		results[i] = r
	}
	return results, "block_part_latencies"
}
