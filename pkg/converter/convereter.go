package converter

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	typeslib "github.com/bft-labs/cometbft-analyzer-types/lib"
	"github.com/bft-labs/cometbft-analyzer-types/pkg/core"
	"github.com/bft-labs/cometbft-analyzer-types/pkg/events"
	"github.com/bft-labs/cometbft-log-etl/lib"
	"github.com/bft-labs/cometbft-log-etl/pkg/parser"
	"github.com/bft-labs/cometbft-log-etl/types"
	cmtcons "github.com/cometbft/cometbft/api/cometbft/consensus/v1"
	v1 "github.com/cometbft/cometbft/api/cometbft/types/v1"
	"github.com/cosmos/gogoproto/proto"
)

// isValidMessageForChannel validates that a message type is appropriate for its channel
func isValidMessageForChannel(msg interface{}, channelID uint64) bool {
	switch channelID {
	case types.VoteChannel:
		_, ok := msg.(*v1.Vote)
		return ok
	case types.DataChannel:
		// Proposals and block parts go through data channel
		switch msg.(type) {
		case *v1.Proposal, *cmtcons.BlockPart,
			v1.Proposal, cmtcons.BlockPart:
			return true
		default:
			return false
		}
	case types.StateChannel:
		// New round step, new valid block, has vote, vote set messages
		switch msg.(type) {
		case *cmtcons.NewRoundStep, *cmtcons.NewValidBlock, *cmtcons.HasVote,
			*cmtcons.VoteSetMaj23, *cmtcons.HasProposalBlockPart, *cmtcons.ProposalPOL,
			cmtcons.NewRoundStep, cmtcons.NewValidBlock, cmtcons.HasVote,
			cmtcons.VoteSetMaj23, cmtcons.HasProposalBlockPart, cmtcons.ProposalPOL:
			return true
		default:
			return false
		}
	case types.VoteSetBitsChannel:
		_, ok := msg.(*cmtcons.VoteSetBits)
		return ok
	case types.MempoolChannel:
		// Currently not handling mempool messages, but would validate here
		return false
	case types.EvidenceChannel, types.BlocksyncChannel, types.PexChannel,
		types.SnapshotChannel, types.ChunkChannel:
		// Allow these channels for now (could add specific validation later)
		return false
	default:
		// Unknown channel - allow but log warning
		return false
	}
}

// isSupportedChannel validates consensus messages for their channels
func isSupportedChannel(cm *cmtcons.Message, channelID uint64) bool {
	switch channelID {
	case types.VoteChannel:
		_, ok := cm.Sum.(*cmtcons.Message_Vote)
		return ok
	case types.DataChannel:
		// Proposals and block parts go through data channel
		switch cm.Sum.(type) {
		case *cmtcons.Message_Proposal, *cmtcons.Message_BlockPart:
			return true
		default:
			return false
		}
	case types.StateChannel:
		// New round step, new valid block, has vote, vote set messages
		switch cm.Sum.(type) {
		case *cmtcons.Message_NewRoundStep, *cmtcons.Message_NewValidBlock, *cmtcons.Message_HasVote,
			*cmtcons.Message_VoteSetMaj23, *cmtcons.Message_HasProposalBlockPart, *cmtcons.Message_ProposalPol:
			return true
		default:
			return false
		}
	case types.VoteSetBitsChannel:
		_, ok := cm.Sum.(*cmtcons.Message_VoteSetBits)
		return ok
	case types.MempoolChannel:
		// We don't handle mempool messages in this converter,
		// TODO: Track mempool messages in a better way
		return false
	case types.EvidenceChannel, types.BlocksyncChannel, types.PexChannel,
		types.SnapshotChannel, types.ChunkChannel:
		// We don't handle these channels in this converter,
		// TODO: Track these channels in a better way
		return false
	default:
		// Should not allow unknown channels, but need to track them
		// TODO: Track unknown channels in a better way
		return false
	}
}

func Convert(raw interface{}) (interface{}, error) {
	switch event := raw.(type) {
	case *types.EnteringNewRound:
		return ConvertToEventEnteringNewRound(event)
	case *types.EnteringNewStep:
		// Skip propose steps since we handle them separately via ProposeStepOurTurn/NotOurTurn
		if event.TargetStep == "propose" {
			return nil, nil // Skip - propose step info comes from ProposeStep events
		}
		return ConvertToSpecificStepEvent(event)
	case *types.ProposeStepOurTurn:
		return ConvertToEventProposeStepOurTurn(event)
	case *types.ProposeStepNotOurTurn:
		return ConvertToEventProposeStepNotOurTurn(event)
	case *types.ReceivedProposal:
		return ConvertToEventReceivedProposal(event)
	case *types.ReceivedCompleteProposalBlock:
		return ConvertToEventReceivedCompleteProposalBlock(event)
	case *types.Send:
		return ConvertToEventSend(event)
	case *types.TrySend:
		return ConvertToEventTrySend(event)
	case *types.ReceivedBytes:
		return ConvertToEventReceiveFromBytes(event)
	case *types.CommittedBlock:
		return ConvertToEventCommittedBlock(event)
	case *types.ScheduledTimeout:
		return ConvertToEventScheduledTimeout(event)
	default:
		return nil, fmt.Errorf("unknown event type: %T", raw)
	}
}

func ConvertToEventEnteringNewRound(enr *types.EnteringNewRound) (*events.EventEnteringNewRound, error) {
	time := lib.MustParseUtcTimestamp(enr.Timestamp)
	ph, pr, ps, err := lib.ParseRoundInfo(enr.Previous)
	if err != nil {
		return nil, err
	}

	return &events.EventEnteringNewRound{
		BaseEvent: events.BaseEvent{
			EventType: events.EventTypeEnteringNewRound,
			Timestamp: time,
		},
		Height:     enr.Height,
		Round:      enr.Round,
		Proposer:   enr.Proposer,
		PrevHeight: ph,
		PrevRound:  pr,
		PrevStep:   ps,
	}, nil
}

func ConvertToSpecificStepEvent(ens *types.EnteringNewStep) (interface{}, error) {
	time := lib.MustParseUtcTimestamp(ens.Timestamp)
	h, r, s, err := lib.ParseRoundInfo(ens.Current)
	if err != nil {
		return nil, err
	}

	// Create step-specific events based on the target step
	// Note: "propose" step is skipped and handled via ProposeStepOurTurn/NotOurTurn events
	switch ens.TargetStep {
	case "prevote":
		return &events.EventEnteringPrevoteStep{
			BaseEvent: events.BaseEvent{
				EventType: events.EventTypeEnteringPrevoteStep,
				Timestamp: time,
			},
			StepChangeInfo: events.StepChangeInfo{
				CurrHeight: h,
				CurrRound:  r,
				CurrStep:   s,
			},
		}, nil
	case "prevote_wait":
		return &events.EventEnteringPrevoteWaitStep{
			BaseEvent: events.BaseEvent{
				EventType: events.EventTypeEnteringPrevoteWaitStep,
				Timestamp: time,
			},
			StepChangeInfo: events.StepChangeInfo{
				CurrHeight: h,
				CurrRound:  r,
				CurrStep:   s,
			},
		}, nil
	case "precommit":
		return &events.EventEnteringPrecommitStep{
			BaseEvent: events.BaseEvent{
				EventType: events.EventTypeEnteringPrecommitStep,
				Timestamp: time,
			},
			StepChangeInfo: events.StepChangeInfo{
				CurrHeight: h,
				CurrRound:  r,
				CurrStep:   s,
			},
		}, nil
	case "precommit_wait":
		return &events.EventEnteringPrecommitWaitStep{
			BaseEvent: events.BaseEvent{
				EventType: events.EventTypeEnteringPrecommitWaitStep,
				Timestamp: time,
			},
			StepChangeInfo: events.StepChangeInfo{
				CurrHeight: h,
				CurrRound:  r,
				CurrStep:   s,
			},
		}, nil
	case "commit":
		return &events.EventEnteringCommitStep{
			BaseEvent: events.BaseEvent{
				EventType: events.EventTypeEnteringCommitStep,
				Timestamp: time,
			},
			StepChangeInfo: events.StepChangeInfo{
				CurrHeight: h,
				CurrRound:  r,
				CurrStep:   s,
			},
		}, nil
	default:
		// Return an error for unknown steps to avoid using deprecated EventStepChange
		return nil, fmt.Errorf("unknown step type: %s", ens.TargetStep)
	}
}

func ConvertToEventProposeStepOurTurn(ps *types.ProposeStepOurTurn) (*events.EventProposeStep, error) {
	time := lib.MustParseUtcTimestamp(ps.Timestamp)

	// Return a simple struct with the turn information for now
	// This will be replaced with proper events.EventProposeStepTurn later
	return &events.EventProposeStep{
		BaseEvent: events.BaseEvent{
			EventType: events.EventTypeProposeStep,
			Timestamp: time,
		},
		Height:    ps.Height,
		Round:     ps.Round,
		Proposer:  ps.Proposer,
		IsOurTurn: true,
	}, nil
}

func ConvertToEventProposeStepNotOurTurn(ps *types.ProposeStepNotOurTurn) (*events.EventProposeStep, error) {
	time := lib.MustParseUtcTimestamp(ps.Timestamp)

	// Return a simple struct with the turn information for now
	// This will be replaced with proper events.EventProposeStepTurn later
	return &events.EventProposeStep{
		BaseEvent: events.BaseEvent{
			EventType: events.EventTypeProposeStep,
			Timestamp: time,
		},
		Height:    ps.Height,
		Round:     ps.Round,
		Proposer:  ps.Proposer,
		IsOurTurn: false,
	}, nil
}

func ConvertToEventReceivedProposal(erp *types.ReceivedProposal) (*events.EventReceivedProposal, error) {
	time := lib.MustParseUtcTimestamp(erp.Timestamp)
	proposal, err := lib.ParseProposalString(erp.Proposal)
	if err != nil {
		return nil, fmt.Errorf("parse proposal string: %w", err)
	}

	return &events.EventReceivedProposal{
		BaseEvent: events.BaseEvent{
			EventType: events.EventTypeReceivedProposal,
			Timestamp: time,
		},
		Proposal: proposal,
		Proposer: erp.Proposer,
	}, nil
}

func ConvertToEventReceivedCompleteProposalBlock(ercp *types.ReceivedCompleteProposalBlock) (*events.EventReceivedCompleteProposalBlock, error) {
	time := lib.MustParseUtcTimestamp(ercp.Timestamp)

	return &events.EventReceivedCompleteProposalBlock{
		BaseEvent: events.BaseEvent{
			EventType: events.EventTypeReceivedCompleteProposalBlock,
			Timestamp: time,
		},
		Hash:   ercp.Hash,
		Height: ercp.Height,
	}, nil
}

func ConvertToEventSend(send *types.Send) (interface{}, error) {
	hexValue, err := hex.DecodeString(send.MsgBytes)
	if err != nil {
		return nil, fmt.Errorf("decode msg bytes: %w", err)
	}
	res, err := parser.DecodeMsgBytes(send.Channel, hexValue)
	if err != nil {
		return nil, fmt.Errorf("decode msg bytes: %w", err)
	}
	recipienPeer := send.Peer
	recipientPeerId := lib.ExtractPeerIdOnly(send.Peer)

	// Channel validation - ensure message type matches expected channel
	channelName := types.GetChannelName(send.Channel)
	if !isValidMessageForChannel(res, send.Channel) {
		return nil, fmt.Errorf("message type %T not valid for channel %s (0x%02x)", res, channelName, send.Channel)
	}

	switch msg := res.(type) {
	case *cmtcons.NewRoundStep:
		evt := &events.EventSendNewRoundStep{
			BaseEvent: events.BaseEvent{
				EventType: events.EventTypeSendNewRoundStep,
				Timestamp: lib.MustParseUtcTimestamp(send.Timestamp),
			},
			Height:                msg.Height,
			Round:                 msg.Round,
			Step:                  typeslib.StepIntToString(msg.Step),
			SecondsSinceStartTime: msg.SecondsSinceStartTime,
			LastCommitRound:       msg.LastCommitRound,
			RecipientInfo: events.RecipientInfo{
				RecipientPeer:   recipienPeer,
				RecipientPeerId: recipientPeerId,
			},
			MsgInfo: events.MsgInfo{
				MsgBytes: hexValue,
			},
		}
		return evt, nil
	case *cmtcons.NewValidBlock:
		evt := &events.EventSendNewValidBlock{
			BaseEvent: events.BaseEvent{
				EventType: events.EventTypeSendNewValidBlock,
				Timestamp: lib.MustParseUtcTimestamp(send.Timestamp),
			},
			Height:             msg.Height,
			Round:              msg.Round,
			BlockPartSetHeader: typeslib.CometPartSetHeaderToPartSetHeader(&msg.BlockPartSetHeader),
			BlockParts: core.BitArray{
				Bits:  msg.BlockParts.Bits,
				Elems: msg.BlockParts.Elems,
			},
			IsCommit: msg.IsCommit,
			RecipientInfo: events.RecipientInfo{
				RecipientPeer:   recipienPeer,
				RecipientPeerId: recipientPeerId,
			},
			MsgInfo: events.MsgInfo{
				MsgBytes: hexValue,
			},
		}
		return evt, nil
	case v1.Proposal:
		evt := &events.EventSendProposal{
			BaseEvent: events.BaseEvent{
				EventType: events.EventTypeSendProposal,
				Timestamp: lib.MustParseUtcTimestamp(send.Timestamp),
			},
			Type:      typeslib.CometSignedMsgTypeToString(int32(msg.Type)),
			Height:    msg.Height,
			Round:     msg.Round,
			PolRound:  msg.PolRound,
			BlockID:   typeslib.CometBlockIDToBlockID(&msg.BlockID),
			Timestamp: msg.Timestamp,
			Signature: hex.EncodeToString(msg.Signature),
			RecipientInfo: events.RecipientInfo{
				RecipientPeer:   recipienPeer,
				RecipientPeerId: recipientPeerId,
			},
			MsgInfo: events.MsgInfo{
				MsgBytes: hexValue,
			},
		}
		return evt, nil
	case *cmtcons.ProposalPOL:
		evt := &events.EventSendProposalPOL{
			BaseEvent: events.BaseEvent{
				EventType: events.EventTypeSendProposalPOL,
				Timestamp: lib.MustParseUtcTimestamp(send.Timestamp),
			},
			Height:           msg.Height,
			ProposalPolRound: msg.ProposalPolRound,
			ProposalPol: core.BitArray{
				Bits:  msg.ProposalPol.Bits,
				Elems: msg.ProposalPol.Elems,
			},
			RecipientInfo: events.RecipientInfo{
				RecipientPeer:   recipienPeer,
				RecipientPeerId: recipientPeerId,
			},
			MsgInfo: events.MsgInfo{
				MsgBytes: hexValue,
			},
		}
		return evt, nil
	case *cmtcons.BlockPart:
		evt := &events.EventSendBlockPart{
			BaseEvent: events.BaseEvent{
				EventType: events.EventTypeSendBlockPart,
				Timestamp: lib.MustParseUtcTimestamp(send.Timestamp),
			},
			Height: msg.Height,
			Round:  msg.Round,
			Part: core.Part{
				Index: msg.Part.Index,
				Bytes: hex.EncodeToString(msg.Part.Bytes),
				Proof: typeslib.CometProofToProof(&msg.Part.Proof),
			},
			RecipientInfo: events.RecipientInfo{
				RecipientPeer:   recipienPeer,
				RecipientPeerId: recipientPeerId,
			},
			MsgInfo: events.MsgInfo{
				MsgBytes: hexValue,
			},
		}
		return evt, nil
	case *v1.Vote:
		evt := &events.EventSendVote{
			BaseEvent: events.BaseEvent{
				EventType: events.EventTypeSendVote,
				Timestamp: lib.MustParseUtcTimestamp(send.Timestamp),
			},
			Vote: typeslib.CometVoteToVote(msg),
			RecipientInfo: events.RecipientInfo{
				RecipientPeer:   recipienPeer,
				RecipientPeerId: recipientPeerId,
			},
			MsgInfo: events.MsgInfo{
				MsgBytes: hexValue,
			},
		}
		return evt, nil
	case *cmtcons.HasVote:
		evt := &events.EventSendHasVote{
			BaseEvent: events.BaseEvent{
				EventType: events.EventTypeSendHasVote,
				Timestamp: lib.MustParseUtcTimestamp(send.Timestamp),
			},
			Height: msg.Height,
			Round:  msg.Round,
			Type:   typeslib.CometSignedMsgTypeToString(int32(msg.Type)),
			Index:  msg.Index,
			RecipientInfo: events.RecipientInfo{
				RecipientPeer:   recipienPeer,
				RecipientPeerId: recipientPeerId,
			},
			MsgInfo: events.MsgInfo{
				MsgBytes: hexValue,
			},
		}
		return evt, nil
	case *cmtcons.VoteSetMaj23:
		evt := &events.EventSendVoteSetMaj23{
			BaseEvent: events.BaseEvent{
				EventType: events.EventTypeSendVoteSetMaj23,
				Timestamp: lib.MustParseUtcTimestamp(send.Timestamp),
			},
			Height:  msg.Height,
			Round:   msg.Round,
			Type:    typeslib.CometSignedMsgTypeToString(int32(msg.Type)),
			BlockID: typeslib.CometBlockIDToBlockID(&msg.BlockID),
			RecipientInfo: events.RecipientInfo{
				RecipientPeer:   recipienPeer,
				RecipientPeerId: recipientPeerId,
			},
			MsgInfo: events.MsgInfo{
				MsgBytes: hexValue,
			},
		}
		return evt, nil
	case *cmtcons.VoteSetBits:
		evt := &events.EventSendVoteSetBits{
			BaseEvent: events.BaseEvent{
				EventType: events.EventTypeSendVoteSetBits,
				Timestamp: lib.MustParseUtcTimestamp(send.Timestamp),
			},
			Height:  msg.Height,
			Round:   msg.Round,
			Type:    typeslib.CometSignedMsgTypeToString(int32(msg.Type)),
			BlockID: typeslib.CometBlockIDToBlockID(&msg.BlockID),
			Votes: core.BitArray{
				Bits:  msg.Votes.Bits,
				Elems: msg.Votes.Elems,
			},
			RecipientInfo: events.RecipientInfo{
				RecipientPeer:   recipienPeer,
				RecipientPeerId: recipientPeerId,
			},
			MsgInfo: events.MsgInfo{
				MsgBytes: hexValue,
			},
		}
		return evt, nil
	case *cmtcons.HasProposalBlockPart:
		return &events.EventSendHasProposalBlockPart{
			BaseEvent: events.BaseEvent{
				EventType: events.EventTypeSendHasProposalBlockPart,
				Timestamp: lib.MustParseUtcTimestamp(send.Timestamp),
			},
			Height: msg.Height,
			Round:  msg.Round,
			Index:  msg.Index,
			RecipientInfo: events.RecipientInfo{
				RecipientPeer:   recipienPeer,
				RecipientPeerId: recipientPeerId,
			},
			MsgInfo: events.MsgInfo{
				MsgBytes: hexValue,
			},
		}, nil
	default:
		// For other message types, we don't have specific event types yet
		return nil, fmt.Errorf("unsupported message type for Send conversion: %T", res)
	}
}

func ConvertToEventTrySend(ts *types.TrySend) (interface{}, error) {
	// msgBytes are hex-encoded uppercase without 0x
	hexValue, err := hex.DecodeString(ts.MsgBytes)
	if err != nil {
		return nil, fmt.Errorf("decode msg bytes: %w", err)
	}
	res, err := parser.DecodeMsgBytes(ts.Channel, hexValue)
	// No peer id available; use conn string as RecipientPeer for trace; RecipientPeerId left empty
	recipienPeer := ts.Peer
	recipientPeerId := lib.ExtractPeerIdOnly(ts.Peer)

	if err != nil || !isValidMessageForChannel(res, ts.Channel) {
		return nil, fmt.Errorf("decode msg bytes or invalid for channel: %w", err)
	}
	// If it happens to be a consensus message, map same as ConvertToEventSend
	switch msg := res.(type) {
	case *cmtcons.NewRoundStep:
		evt := &events.EventSendNewRoundStep{
			BaseEvent: events.BaseEvent{EventType: events.EventTypeSendNewRoundStep, Timestamp: lib.MustParseUtcTimestamp(ts.Timestamp)},
			Height:    msg.Height, Round: msg.Round, Step: typeslib.StepIntToString(msg.Step),
			SecondsSinceStartTime: msg.SecondsSinceStartTime, LastCommitRound: msg.LastCommitRound,
			RecipientInfo: events.RecipientInfo{RecipientPeer: recipienPeer, RecipientPeerId: recipientPeerId},
			MsgInfo:       events.MsgInfo{MsgBytes: hexValue},
		}
		return evt, nil
	case *cmtcons.NewValidBlock:
		evt := &events.EventSendNewValidBlock{
			BaseEvent: events.BaseEvent{EventType: events.EventTypeSendNewValidBlock, Timestamp: lib.MustParseUtcTimestamp(ts.Timestamp)},
			Height:    msg.Height, Round: msg.Round,
			BlockPartSetHeader: typeslib.CometPartSetHeaderToPartSetHeader(&msg.BlockPartSetHeader),
			BlockParts:         core.BitArray{Bits: msg.BlockParts.Bits, Elems: msg.BlockParts.Elems},
			IsCommit:           false,
			RecipientInfo:      events.RecipientInfo{RecipientPeer: recipienPeer, RecipientPeerId: recipientPeerId},
			MsgInfo:            events.MsgInfo{MsgBytes: hexValue},
		}
		return evt, nil
	case *v1.Proposal:
		evt := &events.EventSendProposal{
			BaseEvent: events.BaseEvent{EventType: events.EventTypeSendProposal, Timestamp: lib.MustParseUtcTimestamp(ts.Timestamp)},
			Type:      typeslib.CometSignedMsgTypeToString(int32(msg.Type)),
			Height:    msg.Height, Round: msg.Round, PolRound: msg.PolRound, BlockID: typeslib.CometBlockIDToBlockID(&msg.BlockID),
			Timestamp: msg.Timestamp, Signature: hex.EncodeToString(msg.Signature),
			RecipientInfo: events.RecipientInfo{RecipientPeer: recipienPeer, RecipientPeerId: recipientPeerId},
			MsgInfo:       events.MsgInfo{MsgBytes: hexValue},
		}
		return evt, nil
	case *cmtcons.ProposalPOL:
		evt := &events.EventSendProposalPOL{
			BaseEvent: events.BaseEvent{EventType: events.EventTypeSendProposalPOL, Timestamp: lib.MustParseUtcTimestamp(ts.Timestamp)},
			Height:    msg.Height, ProposalPolRound: msg.ProposalPolRound,
			ProposalPol:   core.BitArray{Bits: msg.ProposalPol.Bits, Elems: msg.ProposalPol.Elems},
			RecipientInfo: events.RecipientInfo{RecipientPeer: recipienPeer, RecipientPeerId: recipientPeerId},
			MsgInfo:       events.MsgInfo{MsgBytes: hexValue},
		}
		return evt, nil
	case *cmtcons.BlockPart:
		evt := &events.EventSendBlockPart{
			BaseEvent: events.BaseEvent{EventType: events.EventTypeSendBlockPart, Timestamp: lib.MustParseUtcTimestamp(ts.Timestamp)},
			Height:    msg.Height, Round: msg.Round,
			Part:          core.Part{Index: msg.Part.Index, Bytes: hex.EncodeToString(msg.Part.Bytes), Proof: typeslib.CometProofToProof(&msg.Part.Proof)},
			RecipientInfo: events.RecipientInfo{RecipientPeer: recipienPeer, RecipientPeerId: recipientPeerId},
			MsgInfo:       events.MsgInfo{MsgBytes: hexValue},
		}
		return evt, nil
	case *cmtcons.HasVote:
		evt := &events.EventSendHasVote{
			BaseEvent: events.BaseEvent{EventType: events.EventTypeSendHasVote, Timestamp: lib.MustParseUtcTimestamp(ts.Timestamp)},
			Height:    msg.Height, Round: msg.Round, Type: typeslib.CometSignedMsgTypeToString(int32(msg.Type)), Index: msg.Index,
			RecipientInfo: events.RecipientInfo{RecipientPeer: recipienPeer, RecipientPeerId: recipientPeerId},
			MsgInfo:       events.MsgInfo{MsgBytes: hexValue},
		}
		return evt, nil
	case *cmtcons.VoteSetMaj23:
		evt := &events.EventSendVoteSetMaj23{
			BaseEvent: events.BaseEvent{EventType: events.EventTypeSendVoteSetMaj23, Timestamp: lib.MustParseUtcTimestamp(ts.Timestamp)},
			Height:    msg.Height, Round: msg.Round, Type: typeslib.CometSignedMsgTypeToString(int32(msg.Type)), BlockID: typeslib.CometBlockIDToBlockID(&msg.BlockID),
			RecipientInfo: events.RecipientInfo{RecipientPeer: recipienPeer, RecipientPeerId: recipientPeerId},
			MsgInfo:       events.MsgInfo{MsgBytes: hexValue},
		}
		return evt, nil
	case *cmtcons.VoteSetBits:
		evt := &events.EventSendVoteSetBits{
			BaseEvent: events.BaseEvent{EventType: events.EventTypeSendVoteSetBits, Timestamp: lib.MustParseUtcTimestamp(ts.Timestamp)},
			Height:    msg.Height, Round: msg.Round, Type: typeslib.CometSignedMsgTypeToString(int32(msg.Type)), BlockID: typeslib.CometBlockIDToBlockID(&msg.BlockID),
			Votes:         core.BitArray{Bits: msg.Votes.Bits, Elems: msg.Votes.Elems},
			RecipientInfo: events.RecipientInfo{RecipientPeer: recipienPeer, RecipientPeerId: recipientPeerId},
			MsgInfo:       events.MsgInfo{MsgBytes: hexValue},
		}
		return evt, nil
	case *cmtcons.HasProposalBlockPart:
		evt := &events.EventSendHasProposalBlockPart{
			BaseEvent: events.BaseEvent{EventType: events.EventTypeSendHasProposalBlockPart, Timestamp: lib.MustParseUtcTimestamp(ts.Timestamp)},
			Height:    msg.Height, Round: msg.Round, Index: msg.Index,
			RecipientInfo: events.RecipientInfo{RecipientPeer: recipienPeer, RecipientPeerId: recipientPeerId},
			MsgInfo:       events.MsgInfo{MsgBytes: hexValue},
		}
		return evt, nil
	default:
		return nil, fmt.Errorf("unsupported message type for TrySend conversion: %T", res)
	}
}

func ConvertToEventReceiveFromBytes(rb *types.ReceivedBytes) (interface{}, error) {
	msgBytes, err := base64.StdEncoding.DecodeString(rb.MsgBytes)
	if err != nil {
		return nil, fmt.Errorf("decode received bytes: %w", err)
	}
	if len(msgBytes) == 0 {
		return nil, nil
	}

	var cm cmtcons.Message
	if err = proto.Unmarshal(msgBytes, &cm); err != nil {
		return nil, err
	}

	senderPeer := rb.Peer
	senderPeerId := lib.ExtractPeerIdOnly(rb.Peer)

	// Channel validation - ensure message type matches expected channel
	if !isSupportedChannel(&cm, rb.ChannelID) {
		return nil, fmt.Errorf("message type %T not valid for channel %s (0x%02x)", cm.Sum, types.GetChannelName(rb.ChannelID), rb.ChannelID)
	}

	switch m := cm.Sum.(type) {
	case *cmtcons.Message_NewRoundStep:
		nrs := m.NewRoundStep
		evt := &events.EventReceivePacketNewRoundStep{
			BaseEvent: events.BaseEvent{
				EventType: events.EventTypeReceivePacketNewRoundStep,
				Timestamp: lib.MustParseUtcTimestamp(rb.Timestamp),
			},
			Height:                nrs.Height,
			Round:                 nrs.Round,
			Step:                  typeslib.StepIntToString(nrs.Step),
			SecondsSinceStartTime: nrs.SecondsSinceStartTime,
			LastCommitRound:       nrs.LastCommitRound,
			SourceInfo: events.SourceInfo{
				SourcePeer:   senderPeer,
				SourcePeerId: senderPeerId,
			},
			MsgInfo: events.MsgInfo{
				MsgBytes: msgBytes,
			},
		}
		return evt, nil
	case *cmtcons.Message_NewValidBlock:
		newValidBlock := m.NewValidBlock
		evt := &events.EventReceivePacketNewValidBlock{
			BaseEvent: events.BaseEvent{
				EventType: events.EventTypeReceivePacketNewValidBlock,
				Timestamp: lib.MustParseUtcTimestamp(rb.Timestamp),
			},
			Height: newValidBlock.Height,
			Round:  newValidBlock.Round,
			BlockPartSetHeader: core.PartSetHeader{
				Total: uint64(newValidBlock.BlockPartSetHeader.Total),
				Hash:  hex.EncodeToString(newValidBlock.BlockPartSetHeader.Hash),
			},
			BlockParts: core.BitArray{
				Bits:  newValidBlock.BlockParts.Bits,
				Elems: newValidBlock.BlockParts.Elems,
			},
			IsCommit: false,
			SourceInfo: events.SourceInfo{
				SourcePeer:   senderPeer,
				SourcePeerId: senderPeerId,
			},
			MsgInfo: events.MsgInfo{
				MsgBytes: msgBytes,
			},
		}
		return evt, nil
	case *cmtcons.Message_Proposal:
		proposal := m.Proposal.Proposal
		evt := &events.EventReceivePacketProposal{
			BaseEvent: events.BaseEvent{
				EventType: events.EventTypeReceivePacketProposal,
				Timestamp: lib.MustParseUtcTimestamp(rb.Timestamp),
			},
			Type:     typeslib.CometSignedMsgTypeToString(int32(proposal.Type)),
			Height:   proposal.Height,
			Round:    proposal.Round,
			PolRound: proposal.PolRound,
			BlockID: core.BlockID{
				Hash: hex.EncodeToString(proposal.BlockID.Hash),
				PartSetHeader: core.PartSetHeader{
					Total: uint64(proposal.BlockID.PartSetHeader.Total),
					Hash:  hex.EncodeToString(proposal.BlockID.PartSetHeader.Hash),
				},
			},
			Timestamp: proposal.Timestamp,
			Signature: hex.EncodeToString(proposal.Signature),
			SourceInfo: events.SourceInfo{
				SourcePeer:   senderPeer,
				SourcePeerId: senderPeerId,
			},
			MsgInfo: events.MsgInfo{
				MsgBytes: msgBytes,
			},
		}
		return evt, nil
	case *cmtcons.Message_ProposalPol:
		proposalPOL := m.ProposalPol
		evt := &events.EventReceivePacketProposalPOL{
			BaseEvent: events.BaseEvent{
				EventType: events.EventTypeReceivePacketProposalPOL,
				Timestamp: lib.MustParseUtcTimestamp(rb.Timestamp),
			},
			Height:           proposalPOL.Height,
			ProposalPolRound: proposalPOL.ProposalPolRound,
			ProposalPol: core.BitArray{
				Bits:  proposalPOL.ProposalPol.Bits,
				Elems: proposalPOL.ProposalPol.Elems,
			},
			SourceInfo: events.SourceInfo{
				SourcePeer:   senderPeer,
				SourcePeerId: senderPeerId,
			},
			MsgInfo: events.MsgInfo{
				MsgBytes: msgBytes,
			},
		}
		return evt, nil
	case *cmtcons.Message_BlockPart:
		blockPart := m.BlockPart
		evt := &events.EventReceivePacketBlockPart{
			BaseEvent: events.BaseEvent{
				EventType: events.EventTypeReceivePacketBlockPart,
				Timestamp: lib.MustParseUtcTimestamp(rb.Timestamp),
			},
			Height: blockPart.Height,
			Round:  blockPart.Round,
			Part: core.Part{
				Index: blockPart.Part.Index,
				Bytes: hex.EncodeToString(blockPart.Part.Bytes),
				Proof: typeslib.CometProofToProof(&blockPart.Part.Proof),
			},
			SourceInfo: events.SourceInfo{
				SourcePeer:   senderPeer,
				SourcePeerId: senderPeerId,
			},
			MsgInfo: events.MsgInfo{
				MsgBytes: msgBytes,
			},
		}
		return evt, nil
	case *cmtcons.Message_Vote:
		vote := m.Vote.Vote
		evt := &events.EventReceivePacketVote{
			BaseEvent: events.BaseEvent{
				EventType: events.EventTypeReceivePacketVote,
				Timestamp: lib.MustParseUtcTimestamp(rb.Timestamp),
			},
			Vote: typeslib.CometVoteToVote(vote),
			SourceInfo: events.SourceInfo{
				SourcePeer:   senderPeer,
				SourcePeerId: senderPeerId,
			},
			MsgInfo: events.MsgInfo{
				MsgBytes: msgBytes,
			},
		}
		return evt, nil
	case *cmtcons.Message_HasVote:
		hasVote := m.HasVote
		evt := &events.EventReceivePacketHasVote{
			BaseEvent: events.BaseEvent{
				EventType: events.EventTypeReceivePacketHasVote,
				Timestamp: lib.MustParseUtcTimestamp(rb.Timestamp),
			},
			Height: hasVote.Height,
			Round:  hasVote.Round,
			Type:   typeslib.CometSignedMsgTypeToString(int32(hasVote.Type)),
			Index:  hasVote.Index,
			SourceInfo: events.SourceInfo{
				SourcePeer:   senderPeer,
				SourcePeerId: senderPeerId,
			},
			MsgInfo: events.MsgInfo{
				MsgBytes: msgBytes,
			},
		}
		return evt, nil
	case *cmtcons.Message_VoteSetMaj23:
		voteSetMaj23 := m.VoteSetMaj23
		evt := &events.EventReceivePacketVoteSetMaj23{
			BaseEvent: events.BaseEvent{
				EventType: events.EventTypeReceivePacketVoteSetMaj23,
				Timestamp: lib.MustParseUtcTimestamp(rb.Timestamp),
			},
			Height:  voteSetMaj23.Height,
			Round:   voteSetMaj23.Round,
			Type:    typeslib.CometSignedMsgTypeToString(int32(voteSetMaj23.Type)),
			BlockID: typeslib.CometBlockIDToBlockID(&voteSetMaj23.BlockID),
			SourceInfo: events.SourceInfo{
				SourcePeer:   senderPeer,
				SourcePeerId: senderPeerId,
			},
			MsgInfo: events.MsgInfo{
				MsgBytes: msgBytes,
			},
		}
		return evt, nil
	case *cmtcons.Message_VoteSetBits:
		voteSetBits := m.VoteSetBits
		evt := &events.EventReceivePacketVoteSetBits{
			BaseEvent: events.BaseEvent{
				EventType: events.EventTypeReceivePacketVoteSetBits,
				Timestamp: lib.MustParseUtcTimestamp(rb.Timestamp),
			},
			Height:  voteSetBits.Height,
			Round:   voteSetBits.Round,
			Type:    typeslib.CometSignedMsgTypeToString(int32(voteSetBits.Type)),
			BlockID: typeslib.CometBlockIDToBlockID(&voteSetBits.BlockID),
			Votes: core.BitArray{
				Bits:  voteSetBits.Votes.Bits,
				Elems: voteSetBits.Votes.Elems,
			},
			SourceInfo: events.SourceInfo{
				SourcePeer:   senderPeer,
				SourcePeerId: senderPeerId,
			},
			MsgInfo: events.MsgInfo{
				MsgBytes: msgBytes,
			},
		}
		return evt, nil
	case *cmtcons.Message_HasProposalBlockPart:
		hasProposalBlockPart := m.HasProposalBlockPart
		evt := &events.EventReceivePacketHasProposalBlockPart{
			BaseEvent: events.BaseEvent{
				EventType: events.EventTypeReceivePacketHasProposalBlockPart,
				Timestamp: lib.MustParseUtcTimestamp(rb.Timestamp),
			},
			Height: hasProposalBlockPart.Height,
			Round:  hasProposalBlockPart.Round,
			Index:  hasProposalBlockPart.Index,
			SourceInfo: events.SourceInfo{
				SourcePeer:   senderPeer,
				SourcePeerId: senderPeerId,
			},
			MsgInfo: events.MsgInfo{
				MsgBytes: msgBytes,
			},
		}
		return evt, nil
	default:
		return nil, fmt.Errorf("unknown message type: %T", m)
	}
}

func ConvertToEventCommittedBlock(cb *types.CommittedBlock) (*events.EventCommittedBlock, error) {
	time := lib.MustParseUtcTimestamp(cb.Timestamp)
	block, err := lib.ParseBlockString(cb.Block)
	if err != nil {
		return nil, fmt.Errorf("parse block ID: %w", err)
	}

	return &events.EventCommittedBlock{
		BaseEvent: events.BaseEvent{
			EventType: events.EventTypeCommittedBlock,
			Timestamp: time,
		},
		Height: cb.Height,
		Block:  block,
	}, nil
}

func ConvertToEventScheduledTimeout(to *types.ScheduledTimeout) (*events.EventScheduledTimeout, error) {
	time := lib.MustParseUtcTimestamp(to.Timestamp)
	step, err := lib.FormatStep(to.Step)
	if err != nil {
		return nil, fmt.Errorf("format step: %w", err)
	}
	return &events.EventScheduledTimeout{
		BaseEvent: events.BaseEvent{
			EventType: events.EventTypeScheduledTimeout,
			Timestamp: time,
		},
		Duration: to.Duration,
		Height:   to.Height,
		Step:     step,
	}, nil
}
