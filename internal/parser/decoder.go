package parser

import (
	"fmt"
	"github.com/bft-labs/cometbft-log-etl/types"
	bcproto "github.com/cometbft/cometbft/api/cometbft/blocksync/v1"
	cmtcons "github.com/cometbft/cometbft/api/cometbft/consensus/v1"
	cmtconsv1beta1 "github.com/cometbft/cometbft/api/cometbft/consensus/v1beta1"
	memproto "github.com/cometbft/cometbft/api/cometbft/mempool/v1"
	p2pproto "github.com/cometbft/cometbft/api/cometbft/p2p/v1"
	ssproto "github.com/cometbft/cometbft/api/cometbft/statesync/v1"
	"github.com/cosmos/gogoproto/proto"
)

// DecodeMsgBytes attempts to decode msgBytes as a consensus message for the given channel.
// For non-consensus channels, returns an error so callers can fallback to raw handling.
func DecodeMsgBytes(channelID uint64, msgBytes []byte) (interface{}, error) {
	if len(msgBytes) == 0 {
		return "", nil
	}

	switch channelID {
	case types.StateChannel, types.DataChannel, types.VoteChannel, types.VoteSetBitsChannel:
		// Consensus messages: try v1 then v1beta1
		var cm cmtcons.Message
		if err := proto.Unmarshal(msgBytes, &cm); err != nil {
			var cmb cmtconsv1beta1.Message
			if err2 := proto.Unmarshal(msgBytes, &cmb); err2 == nil {
				switch m := cmb.Sum.(type) {
				case *cmtconsv1beta1.Message_NewRoundStep:
					return m.NewRoundStep, nil
				case *cmtconsv1beta1.Message_NewValidBlock:
					return m.NewValidBlock, nil
				case *cmtconsv1beta1.Message_Proposal:
					return m.Proposal.Proposal, nil
				case *cmtconsv1beta1.Message_ProposalPol:
					return m.ProposalPol.ProposalPol, nil
				case *cmtconsv1beta1.Message_BlockPart:
					return m.BlockPart, nil
				case *cmtconsv1beta1.Message_Vote:
					return m.Vote.Vote, nil
				case *cmtconsv1beta1.Message_HasVote:
					return m.HasVote, nil
				case *cmtconsv1beta1.Message_VoteSetMaj23:
					return m.VoteSetMaj23, nil
				case *cmtconsv1beta1.Message_VoteSetBits:
					return m.VoteSetBits, nil
				default:
					return nil, fmt.Errorf("unknown consensus v1beta1 message: %T", m)
				}
			}
			return nil, err
		}
		switch m := cm.Sum.(type) {
		case *cmtcons.Message_NewRoundStep:
			return m.NewRoundStep, nil
		case *cmtcons.Message_NewValidBlock:
			return m.NewValidBlock, nil
		case *cmtcons.Message_Proposal:
			return m.Proposal.Proposal, nil
		case *cmtcons.Message_ProposalPol:
			return m.ProposalPol.ProposalPol, nil
		case *cmtcons.Message_BlockPart:
			return m.BlockPart, nil
		case *cmtcons.Message_Vote:
			return m.Vote.Vote, nil
		case *cmtcons.Message_HasVote:
			return m.HasVote, nil
		case *cmtcons.Message_VoteSetMaj23:
			return m.VoteSetMaj23, nil
		case *cmtcons.Message_VoteSetBits:
			return m.VoteSetBits, nil
		case *cmtcons.Message_HasProposalBlockPart:
			return m.HasProposalBlockPart, nil
		default:
			return nil, fmt.Errorf("unknown consensus v1 message: %T", m)
		}

	case types.BlocksyncChannel:
		var bm bcproto.Message
		if err := proto.Unmarshal(msgBytes, &bm); err != nil {
			return nil, err
		}
		return &bm, nil

	case types.MempoolChannel:
		var mm memproto.Message
		if err := proto.Unmarshal(msgBytes, &mm); err != nil {
			return nil, err
		}
		return &mm, nil

	case types.PexChannel:
		var pm p2pproto.Message
		if err := proto.Unmarshal(msgBytes, &pm); err != nil {
			return nil, err
		}
		return &pm, nil

	case types.SnapshotChannel, types.ChunkChannel:
		var sm ssproto.Message
		if err := proto.Unmarshal(msgBytes, &sm); err != nil {
			return nil, err
		}
		return &sm, nil

	case types.EvidenceChannel:
		// No public API message wrapper found; return raw error to allow caller to fallback
		return nil, fmt.Errorf("evidence channel decoding not implemented")
	default:
		return nil, fmt.Errorf("unknown channel 0x%02x", channelID)
	}
}
