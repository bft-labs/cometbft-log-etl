package p2pmessages

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/bft-labs/cometbft-analyzer-types/pkg/events"
)

type P2pMessage struct {
	Status         events.P2pMessageStatus
	SentTime       interface{}
	ReceivedTime   interface{}
	ConfirmedEvent interface{}
}

type Processor struct {
	ctx                  context.Context
	voteMessages         map[string]*P2pMessage
	blockPartMessages    map[string]*P2pMessage
	proposalMessages     map[string]*P2pMessage
	proposalPOLMessages  map[string]*P2pMessage
	newRoundStepMessages map[string]*P2pMessage
	hasVoteMessages      map[string]*P2pMessage
	voteSetMaj23Messages map[string]*P2pMessage
	voteSetBitsMessages  map[string]*P2pMessage
	confirmedEvents      []interface{}
}

func NewP2pMessageProcessor(ctx context.Context) *Processor {
	return &Processor{ctx: ctx,
		voteMessages: make(map[string]*P2pMessage), blockPartMessages: make(map[string]*P2pMessage),
		proposalMessages: make(map[string]*P2pMessage), proposalPOLMessages: make(map[string]*P2pMessage),
		newRoundStepMessages: make(map[string]*P2pMessage), hasVoteMessages: make(map[string]*P2pMessage),
		voteSetMaj23Messages: make(map[string]*P2pMessage), voteSetBitsMessages: make(map[string]*P2pMessage),
		confirmedEvents: make([]interface{}, 0)}
}

func (p *Processor) Process(evt events.Event) error {
	switch e := evt.(type) {
	case *events.EventSendVote:
		return p.processSendVote(e)
	case *events.EventReceivePacketVote:
		return p.processReceiveVote(e)
	case *events.EventSendBlockPart:
		return p.processSendBlockPart(e)
	case *events.EventReceivePacketBlockPart:
		return p.processReceiveBlockPart(e)
	case *events.EventSendProposal:
		return p.processSendProposal(e)
	case *events.EventReceivePacketProposal:
		return p.processReceiveProposal(e)
	case *events.EventSendProposalPOL:
		return p.processSendProposalPOL(e)
	case *events.EventReceivePacketProposalPOL:
		return p.processReceiveProposalPOL(e)
	case *events.EventSendNewRoundStep:
		return p.processSendNewRoundStep(e)
	case *events.EventReceivePacketNewRoundStep:
		return p.processReceiveNewRoundStep(e)
	case *events.EventSendHasVote:
		return p.processSendHasVote(e)
	case *events.EventReceivePacketHasVote:
		return p.processReceiveHasVote(e)
	case *events.EventSendVoteSetMaj23:
		return p.processSendVoteSetMaj23(e)
	case *events.EventReceivePacketVoteSetMaj23:
		return p.processReceiveVoteSetMaj23(e)
	case *events.EventSendVoteSetBits:
		return p.processSendVoteSetBits(e)
	case *events.EventReceivePacketVoteSetBits:
		return p.processReceiveVoteSetBits(e)
	}
	return nil
}

func (p *Processor) processSendVote(e *events.EventSendVote) error {
	key := p.getVoteKey(e.Vote.Height, e.Vote.Round, e.Vote.Type, e.Vote.ValidatorIndex, e.NodeId, e.RecipientPeerId)
	if existing := p.voteMessages[key]; existing != nil && existing.Status == events.P2pMsgStatusReceived {
		recv := existing.ReceivedTime.(*events.EventReceivePacketVote)
		confirmed := &events.EventP2pVote{BaseEvent: events.BaseEvent{EventType: events.EventTypeP2pVote, Timestamp: recv.Timestamp, NodeId: recv.NodeId, ValidatorAddress: recv.ValidatorAddress}, Vote: recv.Vote, P2pInfo: events.P2pInfo{Status: events.P2pMsgStatusConfirmed, SenderPeerId: e.NodeId, RecipientPeerId: e.RecipientPeerId, SentTime: e.Timestamp, ReceivedTime: recv.Timestamp, Latency: recv.Timestamp.Sub(e.Timestamp)}}
		existing.Status = events.P2pMsgStatusConfirmed
		existing.SentTime = e
		existing.ConfirmedEvent = confirmed
		p.confirmedEvents = append(p.confirmedEvents, confirmed)
		p.voteMessages[key] = existing
		return nil
	}
	p.voteMessages[key] = &P2pMessage{Status: events.P2pMsgStatusSent, SentTime: e}
	return nil
}

func (p *Processor) processReceiveVote(e *events.EventReceivePacketVote) error {
	key := p.getVoteKey(e.Vote.Height, e.Vote.Round, e.Vote.Type, e.Vote.ValidatorIndex, e.SourcePeerId, e.NodeId)
	msg := p.voteMessages[key]
	if msg == nil {
		msg = &P2pMessage{Status: events.P2pMsgStatusReceived, ReceivedTime: e}
	} else {
		send := msg.SentTime.(*events.EventSendVote)
		confirmed := &events.EventP2pVote{BaseEvent: events.BaseEvent{EventType: events.EventTypeP2pVote, Timestamp: e.Timestamp, NodeId: e.NodeId, ValidatorAddress: e.ValidatorAddress}, Vote: e.Vote, P2pInfo: events.P2pInfo{Status: events.P2pMsgStatusConfirmed, SenderPeerId: send.NodeId, RecipientPeerId: send.RecipientPeerId, SentTime: send.Timestamp, ReceivedTime: e.Timestamp, Latency: e.Timestamp.Sub(send.Timestamp)}}
		msg.Status = events.P2pMsgStatusConfirmed
		msg.ReceivedTime = e
		msg.ConfirmedEvent = confirmed
		p.confirmedEvents = append(p.confirmedEvents, confirmed)
	}
	p.voteMessages[key] = msg
	return nil
}

func (p *Processor) processSendBlockPart(e *events.EventSendBlockPart) error {
	partHash := hex.EncodeToString(e.Part.Hash())
	key := p.getBlockPartKey(e.Height, e.Round, partHash, e.NodeId, e.RecipientPeerId)
	if existing := p.blockPartMessages[key]; existing != nil && existing.Status == events.P2pMsgStatusReceived {
		recv := existing.ReceivedTime.(*events.EventReceivePacketBlockPart)
		confirmed := &events.EventP2pBlockPart{BaseEvent: events.BaseEvent{EventType: events.EventTypeP2pBlockPart, Timestamp: recv.Timestamp, NodeId: recv.NodeId, ValidatorAddress: recv.ValidatorAddress}, Height: e.Height, Round: e.Round, Part: e.Part, P2pInfo: events.P2pInfo{Status: events.P2pMsgStatusConfirmed, SenderPeerId: e.NodeId, RecipientPeerId: e.RecipientPeerId, SentTime: e.Timestamp, ReceivedTime: recv.Timestamp, Latency: recv.Timestamp.Sub(e.Timestamp)}}
		existing.Status = events.P2pMsgStatusConfirmed
		existing.SentTime = e
		existing.ConfirmedEvent = confirmed
		p.confirmedEvents = append(p.confirmedEvents, confirmed)
		p.blockPartMessages[key] = existing
		return nil
	}
	p.blockPartMessages[key] = &P2pMessage{Status: events.P2pMsgStatusSent, SentTime: e}
	return nil
}

func (p *Processor) processReceiveBlockPart(e *events.EventReceivePacketBlockPart) error {
	partHash := hex.EncodeToString(e.Part.Hash())
	key := p.getBlockPartKey(e.Height, e.Round, partHash, e.SourcePeerId, e.NodeId)
	msg := p.blockPartMessages[key]
	if msg == nil {
		msg = &P2pMessage{Status: events.P2pMsgStatusReceived, ReceivedTime: e}
	} else {
		send := msg.SentTime.(*events.EventSendBlockPart)
		confirmed := &events.EventP2pBlockPart{BaseEvent: events.BaseEvent{EventType: events.EventTypeP2pBlockPart, Timestamp: e.Timestamp, NodeId: e.NodeId, ValidatorAddress: e.ValidatorAddress}, Height: e.Height, Round: e.Round, Part: e.Part, P2pInfo: events.P2pInfo{Status: events.P2pMsgStatusConfirmed, SenderPeerId: send.NodeId, RecipientPeerId: send.RecipientPeerId, SentTime: send.Timestamp, ReceivedTime: e.Timestamp, Latency: e.Timestamp.Sub(send.Timestamp)}}
		msg.Status = events.P2pMsgStatusConfirmed
		msg.ReceivedTime = e
		msg.ConfirmedEvent = confirmed
		p.confirmedEvents = append(p.confirmedEvents, confirmed)
	}
	p.blockPartMessages[key] = msg
	return nil
}

func (p *Processor) processSendProposal(e *events.EventSendProposal) error {
	key := p.getProposalKey(e.Height, e.Round, e.BlockID.Hash, e.NodeId, e.RecipientPeerId)
	if existing := p.proposalMessages[key]; existing != nil && existing.Status == events.P2pMsgStatusReceived {
		recv := existing.ReceivedTime.(*events.EventReceivePacketProposal)
		confirmed := &events.EventP2pProposal{BaseEvent: events.BaseEvent{EventType: events.EventTypeP2pProposal, Timestamp: recv.Timestamp, NodeId: recv.NodeId, ValidatorAddress: recv.ValidatorAddress}, Height: recv.Height, Round: recv.Round, PolRound: recv.PolRound, BlockID: recv.BlockID, Timestamp: recv.Timestamp, Signature: recv.Signature, P2pInfo: events.P2pInfo{Status: events.P2pMsgStatusConfirmed, SenderPeerId: e.NodeId, RecipientPeerId: e.RecipientPeerId, SentTime: e.Timestamp, ReceivedTime: recv.Timestamp, Latency: recv.Timestamp.Sub(e.Timestamp)}}
		existing.Status = events.P2pMsgStatusConfirmed
		existing.SentTime = e
		existing.ConfirmedEvent = confirmed
		p.confirmedEvents = append(p.confirmedEvents, confirmed)
		p.proposalMessages[key] = existing
		return nil
	}
	p.proposalMessages[key] = &P2pMessage{Status: events.P2pMsgStatusSent, SentTime: e}
	return nil
}

func (p *Processor) processReceiveProposal(e *events.EventReceivePacketProposal) error {
	key := p.getProposalKey(e.Height, e.Round, e.BlockID.Hash, e.SourcePeerId, e.NodeId)
	msg := p.proposalMessages[key]
	if msg == nil {
		msg = &P2pMessage{Status: events.P2pMsgStatusReceived, ReceivedTime: e}
	} else {
		send := msg.SentTime.(*events.EventSendProposal)
		confirmed := &events.EventP2pProposal{BaseEvent: events.BaseEvent{EventType: events.EventTypeP2pProposal, Timestamp: e.Timestamp, NodeId: e.NodeId, ValidatorAddress: e.ValidatorAddress}, Height: e.Height, Round: e.Round, PolRound: e.PolRound, BlockID: e.BlockID, Timestamp: e.Timestamp, Signature: e.Signature, P2pInfo: events.P2pInfo{Status: events.P2pMsgStatusConfirmed, SenderPeerId: send.NodeId, RecipientPeerId: send.RecipientPeerId, SentTime: send.Timestamp, ReceivedTime: e.Timestamp, Latency: e.Timestamp.Sub(send.Timestamp)}}
		msg.Status = events.P2pMsgStatusConfirmed
		msg.ReceivedTime = e
		msg.ConfirmedEvent = confirmed
		p.confirmedEvents = append(p.confirmedEvents, confirmed)
	}
	p.proposalMessages[key] = msg
	return nil
}

func (p *Processor) processSendProposalPOL(e *events.EventSendProposalPOL) error {
	key := p.getProposalPOLKey(e.Height, e.ProposalPolRound, e.NodeId, e.RecipientPeerId)
	if existing := p.proposalPOLMessages[key]; existing != nil && existing.Status == events.P2pMsgStatusReceived {
		recv := existing.ReceivedTime.(*events.EventReceivePacketProposalPOL)
		confirmed := &events.EventP2pProposalPOL{BaseEvent: events.BaseEvent{EventType: events.EventTypeP2pProposalPOL, Timestamp: recv.Timestamp, NodeId: recv.NodeId, ValidatorAddress: recv.ValidatorAddress}, Height: recv.Height, ProposalPolRound: recv.ProposalPolRound, ProposalPol: recv.ProposalPol, P2pInfo: events.P2pInfo{Status: events.P2pMsgStatusConfirmed, SenderPeerId: e.NodeId, RecipientPeerId: e.RecipientPeerId, SentTime: e.Timestamp, ReceivedTime: recv.Timestamp, Latency: recv.Timestamp.Sub(e.Timestamp)}}
		existing.Status = events.P2pMsgStatusConfirmed
		existing.SentTime = e
		existing.ConfirmedEvent = confirmed
		p.confirmedEvents = append(p.confirmedEvents, confirmed)
		p.proposalPOLMessages[key] = existing
		return nil
	}
	p.proposalPOLMessages[key] = &P2pMessage{Status: events.P2pMsgStatusSent, SentTime: e}
	return nil
}

func (p *Processor) processReceiveProposalPOL(e *events.EventReceivePacketProposalPOL) error {
	key := p.getProposalPOLKey(e.Height, e.ProposalPolRound, e.SourcePeerId, e.NodeId)
	msg := p.proposalPOLMessages[key]
	if msg == nil {
		msg = &P2pMessage{Status: events.P2pMsgStatusReceived, ReceivedTime: e}
	} else {
		send := msg.SentTime.(*events.EventSendProposalPOL)
		confirmed := &events.EventP2pProposalPOL{BaseEvent: events.BaseEvent{EventType: events.EventTypeP2pProposalPOL, Timestamp: e.Timestamp, NodeId: e.NodeId, ValidatorAddress: e.ValidatorAddress}, Height: e.Height, ProposalPolRound: e.ProposalPolRound, ProposalPol: e.ProposalPol, P2pInfo: events.P2pInfo{Status: events.P2pMsgStatusConfirmed, SenderPeerId: send.NodeId, RecipientPeerId: send.RecipientPeerId, SentTime: send.Timestamp, ReceivedTime: e.Timestamp, Latency: e.Timestamp.Sub(send.Timestamp)}}
		msg.Status = events.P2pMsgStatusConfirmed
		msg.ReceivedTime = e
		msg.ConfirmedEvent = confirmed
		p.confirmedEvents = append(p.confirmedEvents, confirmed)
	}
	p.proposalPOLMessages[key] = msg
	return nil
}

func (p *Processor) processSendNewRoundStep(e *events.EventSendNewRoundStep) error {
	key := p.getNewRoundStepKey(e.Height, e.Round, e.Step, e.NodeId, e.RecipientPeerId)
	if existing := p.newRoundStepMessages[key]; existing != nil && existing.Status == events.P2pMsgStatusReceived {
		recv := existing.ReceivedTime.(*events.EventReceivePacketNewRoundStep)
		confirmed := &events.EventP2pNewRoundStep{BaseEvent: events.BaseEvent{EventType: events.EventTypeP2pNewRoundStep, Timestamp: recv.Timestamp, NodeId: recv.NodeId, ValidatorAddress: recv.ValidatorAddress}, Height: e.Height, Round: e.Round, Step: e.Step, SecondsSinceStartTime: int32(recv.SecondsSinceStartTime), LastCommitRound: recv.LastCommitRound, P2pInfo: events.P2pInfo{Status: events.P2pMsgStatusConfirmed, SenderPeerId: e.NodeId, RecipientPeerId: e.RecipientPeerId, SentTime: e.Timestamp, ReceivedTime: recv.Timestamp, Latency: recv.Timestamp.Sub(e.Timestamp)}}
		existing.Status = events.P2pMsgStatusConfirmed
		existing.SentTime = e
		existing.ConfirmedEvent = confirmed
		p.confirmedEvents = append(p.confirmedEvents, confirmed)
		p.newRoundStepMessages[key] = existing
		return nil
	}
	p.newRoundStepMessages[key] = &P2pMessage{Status: events.P2pMsgStatusSent, SentTime: e}
	return nil
}

func (p *Processor) processReceiveNewRoundStep(e *events.EventReceivePacketNewRoundStep) error {
	key := p.getNewRoundStepKey(e.Height, e.Round, e.Step, e.SourcePeerId, e.NodeId)
	msg := p.newRoundStepMessages[key]
	if msg == nil {
		msg = &P2pMessage{Status: events.P2pMsgStatusReceived, ReceivedTime: e}
	} else {
		send := msg.SentTime.(*events.EventSendNewRoundStep)
		confirmed := &events.EventP2pNewRoundStep{BaseEvent: events.BaseEvent{EventType: events.EventTypeP2pNewRoundStep, Timestamp: e.Timestamp, NodeId: e.NodeId, ValidatorAddress: e.ValidatorAddress}, Height: e.Height, Round: e.Round, Step: e.Step, SecondsSinceStartTime: int32(e.SecondsSinceStartTime), LastCommitRound: e.LastCommitRound, P2pInfo: events.P2pInfo{Status: events.P2pMsgStatusConfirmed, SenderPeerId: send.NodeId, RecipientPeerId: send.RecipientPeerId, SentTime: send.Timestamp, ReceivedTime: e.Timestamp, Latency: e.Timestamp.Sub(send.Timestamp)}}
		msg.Status = events.P2pMsgStatusConfirmed
		msg.ReceivedTime = e
		msg.ConfirmedEvent = confirmed
		p.confirmedEvents = append(p.confirmedEvents, confirmed)
	}
	p.newRoundStepMessages[key] = msg
	return nil
}

func (p *Processor) processSendHasVote(e *events.EventSendHasVote) error {
	key := p.getHasVoteKey(e.Height, e.Round, e.Type, e.Index, e.NodeId, e.RecipientPeerId)
	if existing := p.hasVoteMessages[key]; existing != nil && existing.Status == events.P2pMsgStatusReceived {
		recv := existing.ReceivedTime.(*events.EventReceivePacketHasVote)
		confirmed := &events.EventP2pHasVote{BaseEvent: events.BaseEvent{EventType: events.EventTypeP2pHasVote, Timestamp: recv.Timestamp, NodeId: recv.NodeId, ValidatorAddress: recv.ValidatorAddress}, Height: recv.Height, Round: recv.Round, Type: recv.Type, Index: recv.Index, P2pInfo: events.P2pInfo{Status: events.P2pMsgStatusConfirmed, SenderPeerId: e.NodeId, RecipientPeerId: e.RecipientPeerId, SentTime: e.Timestamp, ReceivedTime: recv.Timestamp, Latency: recv.Timestamp.Sub(e.Timestamp)}}
		existing.Status = events.P2pMsgStatusConfirmed
		existing.SentTime = e
		existing.ConfirmedEvent = confirmed
		p.confirmedEvents = append(p.confirmedEvents, confirmed)
		p.hasVoteMessages[key] = existing
		return nil
	}
	p.hasVoteMessages[key] = &P2pMessage{Status: events.P2pMsgStatusSent, SentTime: e}
	return nil
}

func (p *Processor) processReceiveHasVote(e *events.EventReceivePacketHasVote) error {
	key := p.getHasVoteKey(e.Height, e.Round, e.Type, e.Index, e.SourcePeerId, e.NodeId)
	msg := p.hasVoteMessages[key]
	if msg == nil {
		msg = &P2pMessage{Status: events.P2pMsgStatusReceived, ReceivedTime: e}
	} else {
		send := msg.SentTime.(*events.EventSendHasVote)
		confirmed := &events.EventP2pHasVote{BaseEvent: events.BaseEvent{EventType: events.EventTypeP2pHasVote, Timestamp: e.Timestamp, NodeId: e.NodeId, ValidatorAddress: e.ValidatorAddress}, Height: e.Height, Round: e.Round, Type: e.Type, Index: e.Index, P2pInfo: events.P2pInfo{Status: events.P2pMsgStatusConfirmed, SenderPeerId: send.NodeId, RecipientPeerId: send.RecipientPeerId, SentTime: send.Timestamp, ReceivedTime: e.Timestamp, Latency: e.Timestamp.Sub(send.Timestamp)}}
		msg.Status = events.P2pMsgStatusConfirmed
		msg.ReceivedTime = e
		msg.ConfirmedEvent = confirmed
		p.confirmedEvents = append(p.confirmedEvents, confirmed)
	}
	p.hasVoteMessages[key] = msg
	return nil
}

func (p *Processor) processSendVoteSetMaj23(e *events.EventSendVoteSetMaj23) error {
	key := p.getVoteSetMaj23Key(e.Height, e.Round, e.Type, e.BlockID.Hash, e.NodeId, e.RecipientPeerId)
	if existing := p.voteSetMaj23Messages[key]; existing != nil && existing.Status == events.P2pMsgStatusReceived {
		recv := existing.ReceivedTime.(*events.EventReceivePacketVoteSetMaj23)
		confirmed := &events.EventP2pVoteSetMaj23{BaseEvent: events.BaseEvent{EventType: events.EventTypeSendVoteSetMaj23, Timestamp: recv.Timestamp, NodeId: recv.NodeId, ValidatorAddress: recv.ValidatorAddress}, Height: recv.Height, Round: recv.Round, Type: recv.Type, BlockID: &recv.BlockID, P2pInfo: events.P2pInfo{Status: events.P2pMsgStatusConfirmed, SenderPeerId: e.NodeId, RecipientPeerId: e.RecipientPeerId, SentTime: e.Timestamp, ReceivedTime: recv.Timestamp, Latency: recv.Timestamp.Sub(e.Timestamp)}}
		existing.Status = events.P2pMsgStatusConfirmed
		existing.SentTime = e
		existing.ConfirmedEvent = confirmed
		p.confirmedEvents = append(p.confirmedEvents, confirmed)
		p.voteSetMaj23Messages[key] = existing
		return nil
	}
	p.voteSetMaj23Messages[key] = &P2pMessage{Status: events.P2pMsgStatusSent, SentTime: e}
	return nil
}

func (p *Processor) processReceiveVoteSetMaj23(e *events.EventReceivePacketVoteSetMaj23) error {
	key := p.getVoteSetMaj23Key(e.Height, e.Round, e.Type, e.BlockID.Hash, e.SourcePeerId, e.NodeId)
	msg := p.voteSetMaj23Messages[key]
	if msg == nil {
		msg = &P2pMessage{Status: events.P2pMsgStatusReceived, ReceivedTime: e}
	} else {
		send := msg.SentTime.(*events.EventSendVoteSetMaj23)
		confirmed := &events.EventP2pVoteSetMaj23{BaseEvent: events.BaseEvent{EventType: events.EventTypeSendVoteSetMaj23, Timestamp: e.Timestamp, NodeId: e.NodeId, ValidatorAddress: e.ValidatorAddress}, Height: e.Height, Round: e.Round, Type: e.Type, BlockID: &e.BlockID, P2pInfo: events.P2pInfo{Status: events.P2pMsgStatusConfirmed, SenderPeerId: send.NodeId, RecipientPeerId: send.RecipientPeerId, SentTime: send.Timestamp, ReceivedTime: e.Timestamp, Latency: e.Timestamp.Sub(send.Timestamp)}}
		msg.Status = events.P2pMsgStatusConfirmed
		msg.ReceivedTime = e
		msg.ConfirmedEvent = confirmed
		p.confirmedEvents = append(p.confirmedEvents, confirmed)
	}
	p.voteSetMaj23Messages[key] = msg
	return nil
}

func (p *Processor) processSendVoteSetBits(e *events.EventSendVoteSetBits) error {
	key := p.getVoteSetBitsKey(e.Height, e.Round, e.Type, e.BlockID.Hash, e.NodeId, e.RecipientPeerId)
	if existing := p.voteSetBitsMessages[key]; existing != nil && existing.Status == events.P2pMsgStatusReceived {
		recv := existing.ReceivedTime.(*events.EventReceivePacketVoteSetBits)
		confirmed := &events.EventP2pVoteSetBits{BaseEvent: events.BaseEvent{EventType: events.EventTypeP2pVoteSetBits, Timestamp: recv.Timestamp, NodeId: recv.NodeId, ValidatorAddress: recv.ValidatorAddress}, Height: recv.Height, Round: recv.Round, Type: recv.Type, BlockID: &recv.BlockID, Votes: recv.Votes, P2pInfo: events.P2pInfo{Status: events.P2pMsgStatusConfirmed, SenderPeerId: e.NodeId, RecipientPeerId: e.RecipientPeerId, SentTime: e.Timestamp, ReceivedTime: recv.Timestamp, Latency: recv.Timestamp.Sub(e.Timestamp)}}
		existing.Status = events.P2pMsgStatusConfirmed
		existing.SentTime = e
		existing.ConfirmedEvent = confirmed
		p.confirmedEvents = append(p.confirmedEvents, confirmed)
		p.voteSetBitsMessages[key] = existing
		return nil
	}
	p.voteSetBitsMessages[key] = &P2pMessage{Status: events.P2pMsgStatusSent, SentTime: e}
	return nil
}

func (p *Processor) processReceiveVoteSetBits(e *events.EventReceivePacketVoteSetBits) error {
	key := p.getVoteSetBitsKey(e.Height, e.Round, e.Type, e.BlockID.Hash, e.SourcePeerId, e.NodeId)
	msg := p.voteSetBitsMessages[key]
	if msg == nil {
		msg = &P2pMessage{Status: events.P2pMsgStatusReceived, ReceivedTime: e}
	} else {
		send := msg.SentTime.(*events.EventSendVoteSetBits)
		confirmed := &events.EventP2pVoteSetBits{BaseEvent: events.BaseEvent{EventType: events.EventTypeP2pVoteSetBits, Timestamp: e.Timestamp, NodeId: e.NodeId, ValidatorAddress: e.ValidatorAddress}, Height: e.Height, Round: e.Round, Type: e.Type, BlockID: &e.BlockID, Votes: e.Votes, P2pInfo: events.P2pInfo{Status: events.P2pMsgStatusConfirmed, SenderPeerId: send.NodeId, RecipientPeerId: send.RecipientPeerId, SentTime: send.Timestamp, ReceivedTime: e.Timestamp, Latency: e.Timestamp.Sub(send.Timestamp)}}
		msg.Status = events.P2pMsgStatusConfirmed
		msg.ReceivedTime = e
		msg.ConfirmedEvent = confirmed
		p.confirmedEvents = append(p.confirmedEvents, confirmed)
	}
	p.voteSetBitsMessages[key] = msg
	return nil
}

func (p *Processor) getVoteKey(height uint64, round uint64, voteType string, validatorIndex uint64, sender, receiver string) string {
	return fmt.Sprintf("vote:%d:%d:%s:%d:%s:%s", height, round, voteType, validatorIndex, sender, receiver)
}
func (p *Processor) getBlockPartKey(height int64, round int32, partHashHex, sender, receiver string) string {
	return fmt.Sprintf("blockpart:%d:%d:%s:%s:%s", height, round, partHashHex, sender, receiver)
}
func (p *Processor) getProposalKey(height int64, round int32, blockHash, sender, receiver string) string {
	return fmt.Sprintf("proposal:%d:%d:%s:%s:%s", height, round, blockHash, sender, receiver)
}
func (p *Processor) getProposalPOLKey(height int64, polRound int32, sender, receiver string) string {
	return fmt.Sprintf("proposalpol:%d:%d:%s:%s", height, polRound, sender, receiver)
}
func (p *Processor) getNewRoundStepKey(height int64, round int32, step, sender, receiver string) string {
	return fmt.Sprintf("newroundstep:%d:%d:%s:%s:%s", height, round, step, sender, receiver)
}
func (p *Processor) getHasVoteKey(height int64, round int32, voteType string, index int32, sender, receiver string) string {
	return fmt.Sprintf("hasvote:%d:%d:%s:%d:%s:%s", height, round, voteType, index, sender, receiver)
}
func (p *Processor) getVoteSetMaj23Key(height int64, round int32, voteType, blockHash, sender, receiver string) string {
	return fmt.Sprintf("votesetmaj23:%d:%d:%s:%s:%s:%s", height, round, voteType, blockHash, sender, receiver)
}
func (p *Processor) getVoteSetBitsKey(height int64, round int32, voteType, blockHash, sender, receiver string) string {
	return fmt.Sprintf("votesetbits:%d:%d:%s:%s:%s:%s", height, round, voteType, blockHash, sender, receiver)
}

func (p *Processor) GetResults() ([]interface{}, string) { return p.confirmedEvents, "p2p_messages" }
