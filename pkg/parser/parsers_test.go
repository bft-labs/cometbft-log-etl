package parser

import (
	"github.com/bft-labs/cometbft-log-etl/types"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestDispatch(t *testing.T) {
	// Test cases for each parser function
	tests := []struct {
		name     string
		raw      []byte
		expected interface{}
	}{
		{
			name: "Validator",
			raw:  []byte(`{"_msg":"This node is a validator","addr":"27F16B56F09EC2EE3E92A081446123F139BC2926","level":"info","module":"consensus","pubKey":"PubKeyEd25519{E75447F53596A7F0220AC24B76830450FA2F80489B1FF32931049CF872CDB61A}","ts":"2025-06-08T01:24:18.069056Z"}`),
			expected: &types.Validator{
				Address:   "27F16B56F09EC2EE3E92A081446123F139BC2926",
				PubKey:    "PubKeyEd25519{E75447F53596A7F0220AC24B76830450FA2F80489B1FF32931049CF872CDB61A}",
				Timestamp: "2025-06-08T01:24:18.069056Z",
				Module:    "consensus",
				Level:     "info",
			},
		},
		{
			name: "P2pNodeID",
			raw:  []byte(`{"ID":"c9124bcc8a68b332bb01164bda1cee1b2e2892fc","_msg":"P2P Node ID","file":"mytestnet/node0/config/node_key.json","level":"info","module":"p2p","ts":"2025-06-08T01:24:18.117039Z"}`),
			expected: &types.P2pNodeID{
				ID:        "c9124bcc8a68b332bb01164bda1cee1b2e2892fc",
				File:      "mytestnet/node0/config/node_key.json",
				Timestamp: "2025-06-08T01:24:18.117039Z",
				Module:    "p2p",
				Level:     "info",
			},
		},
		{
			name: "ProposeStepNotTurnToPropose",
			raw:  []byte(`{"_msg":"Propose step; not our turn to propose","height":1,"level":"debug","module":"consensus","proposer":"14723CA6837129C61A873631CF7C865AEBBF63B1","round":0,"ts":"2025-06-08T01:24:19.125509Z"}`),
			expected: &types.ProposeStepNotOurTurn{
				Height:    1,
				Round:     0,
				Proposer:  "14723CA6837129C61A873631CF7C865AEBBF63B1",
				Timestamp: "2025-06-08T01:24:19.125509Z",
				Module:    "consensus",
				Level:     "debug",
			},
		},
		{
			name: "ProposeStepTurnToPropose",
			raw:  []byte(`{"_msg":"Propose step; our turn to propose","height":2,"level":"debug","module":"consensus","proposer":"27F16B56F09EC2EE3E92A081446123F139BC2926","round":0,"ts":"2025-06-08T01:24:22.425463Z"}`),
			expected: &types.ProposeStepOurTurn{
				Height:    2,
				Round:     0,
				Proposer:  "27F16B56F09EC2EE3E92A081446123F139BC2926",
				Timestamp: "2025-06-08T01:24:22.425463Z",
				Module:    "consensus",
				Level:     "debug",
			},
		},
		{
			name: "SignedProposal",
			raw:  []byte(`{"_msg":"Signed proposal","height":2,"level":"debug","module":"consensus","proposal":"Proposal{2/0 (B5B87A0CB4173DB4843C223FDD880E46117D7FD2D45B0FDCA54F5A71A3EAC56F:1:5CF256DF4F94, -1) 5F9FD84D580E @ 2025-06-08T01:24:21.316031Z}","round":0,"ts":"2025-06-08T01:24:22.430524Z"}`),
			expected: &types.SignedProposal{
				Height:    2,
				Round:     0,
				Proposal:  "Proposal{2/0 (B5B87A0CB4173DB4843C223FDD880E46117D7FD2D45B0FDCA54F5A71A3EAC56F:1:5CF256DF4F94, -1) 5F9FD84D580E @ 2025-06-08T01:24:21.316031Z}",
				Timestamp: "2025-06-08T01:24:22.430524Z",
				Module:    "consensus",
				Level:     "debug",
			},
		},
		{
			name: "EnteringNewRound",
			raw:  []byte(`{"_msg":"Entering new round","height":5,"level":"debug","module":"consensus","previous":"5/0/RoundStepNewHeight","proposer":"14723CA6837129C61A873631CF7C865AEBBF63B1","round":0,"ts":"2025-06-08T01:24:26.326276Z"}`),
			expected: &types.EnteringNewRound{
				Height:    5,
				Module:    "consensus",
				Previous:  "5/0/RoundStepNewHeight",
				Proposer:  "14723CA6837129C61A873631CF7C865AEBBF63B1",
				Round:     0,
				Timestamp: "2025-06-08T01:24:26.326276Z",
				Level:     "debug",
			},
		},
		{
			name: "EnteringNewRoundWithInvalidArgs",
			raw:  []byte(`{"_msg":"Entering new round with invalid args","current":"2/0/RoundStepPrecommit","height":2,"level":"debug","module":"consensus","round":0,"ts":"2025-06-08T01:24:22.717919Z"}`),
			expected: &types.EnteringNewRound{
				Current:   "2/0/RoundStepPrecommit",
				Height:    2,
				Round:     0,
				Timestamp: "2025-06-08T01:24:22.717919Z",
				Module:    "consensus",
				Level:     "debug",
			},
		},
		{
			name: "EnteringPrevoteStep",
			raw:  []byte(`{"_msg":"Entering prevote step","current":"4/0/RoundStepPropose","height":4,"level":"debug","module":"consensus","round":0,"ts":"2025-06-08T01:24:25.115023Z"}`),
			expected: &types.EnteringNewStep{
				Current:    "4/0/RoundStepPropose",
				TargetStep: "prevote",
				Height:     4,
				Round:      0,
				Timestamp:  "2025-06-08T01:24:25.115023Z",
				Module:     "consensus",
				Level:      "debug",
			},
		},
		{
			name: "EnteringPrecommitStep",
			raw:  []byte(`{"_msg":"Entering precommit step","current":"4/0/RoundStepPrevote","height":4,"level":"debug","module":"consensus","round":0,"ts":"2025-06-08T01:24:25.212518Z"}`),
			expected: &types.EnteringNewStep{
				Current:    "4/0/RoundStepPrevote",
				TargetStep: "precommit",
				Height:     4,
				Round:      0,
				Timestamp:  "2025-06-08T01:24:25.212518Z",
				Module:     "consensus",
				Level:      "debug",
			},
		},
		{
			name: "LockingBlock",
			raw:  []byte(`{"_msg":"Precommit step: +2/3 prevoted proposal block; locking","hash":"B5B87A0CB4173DB4843C223FDD880E46117D7FD2D45B0FDCA54F5A71A3EAC56F","height":2,"level":"debug","module":"consensus","round":0,"ts":"2025-06-08T01:24:22.620082Z"}`),
			expected: &types.LockingBlock{
				Hash:      "B5B87A0CB4173DB4843C223FDD880E46117D7FD2D45B0FDCA54F5A71A3EAC56F",
				Height:    2,
				Round:     0,
				Timestamp: "2025-06-08T01:24:22.620082Z",
				Module:    "consensus",
				Level:     "debug",
			},
		},
		{
			name: "EnteringPrecommitStepWithInvalidArgs",
			raw:  []byte(`{"_msg":"Entering precommit step with invalid args","current":"2/0/RoundStepPrecommit","height":2,"level":"debug","module":"consensus","round":0,"ts":"2025-06-08T01:24:22.64294Z"}`),
			expected: &types.EnteringNewStep{
				Current:    "2/0/RoundStepPrecommit",
				TargetStep: "precommit",
				Height:     2,
				Round:      0,
				Timestamp:  "2025-06-08T01:24:22.64294Z",
				Module:     "consensus",
				Level:      "debug",
			},
		},
		{
			name: "EnteringPrecommitStep",
			raw:  []byte(`{"_msg":"Entering precommit step","current":"4/0/RoundStepPrevote","height":4,"level":"debug","module":"consensus","round":0,"ts":"2025-06-08T01:24:25.212518Z"}`),
			expected: &types.EnteringNewStep{
				Current:    "4/0/RoundStepPrevote",
				TargetStep: "precommit",
				Height:     4,
				Round:      0,
				Timestamp:  "2025-06-08T01:24:25.212518Z",
				Module:     "consensus",
				Level:      "debug",
			},
		},
		{
			name: "AddingVote",
			raw:  []byte(`{"_msg":"Adding vote","cs_height":2,"extLen":0,"extSigLen":0,"level":"debug","module":"consensus","ts":"2025-06-08T01:24:22.449922Z","val_index":1,"vote_height":2,"vote_type":"SIGNED_MSG_TYPE_PREVOTE"}`),
			expected: &types.AddingVote{
				ConsensusHeight:              2,
				VoteExtensionLength:          0,
				VoteExtensionSignatureLength: 0,
				Module:                       "consensus",
				Timestamp:                    "2025-06-08T01:24:22.449922Z",
				ValidatorIndex:               1,
				VoteHeight:                   2,
				VoteType:                     "SIGNED_MSG_TYPE_PREVOTE",
				Level:                        "debug",
			},
		},
		{
			name: "AddedVoteToPrevote",
			raw:  []byte(`{"_msg":"Added vote to prevote","level":"debug","module":"consensus","prevotes":"VoteSet{H:4 R:0 T:SIGNED_MSG_TYPE_PREVOTE +2/3:C3C61C93E975E4F6321BA3ACFBBCA6CB9678959A5B7F932A8D9B21BD5E42AF8C:1:87DFE237E181(1) BA{4:xxxx} map[]}","ts":"2025-06-08T01:24:25.237969Z","vote":"Vote{0:14723CA68371 4/00/SIGNED_MSG_TYPE_PREVOTE(Prevote) C3C61C93E975 2FD9C7900471 000000000000 @ 2025-06-08T01:24:25.135287Z}"}`),
			expected: &types.AddedVoteToPrevote{
				Module:    "consensus",
				Timestamp: "2025-06-08T01:24:25.237969Z",
				Vote:      "Vote{0:14723CA68371 4/00/SIGNED_MSG_TYPE_PREVOTE(Prevote) C3C61C93E975 2FD9C7900471 000000000000 @ 2025-06-08T01:24:25.135287Z}",
				Prevotes:  "VoteSet{H:4 R:0 T:SIGNED_MSG_TYPE_PREVOTE +2/3:C3C61C93E975E4F6321BA3ACFBBCA6CB9678959A5B7F932A8D9B21BD5E42AF8C:1:87DFE237E181(1) BA{4:xxxx} map[]}",
				Level:     "debug",
			},
		},
		{
			name: "AddedVoteToPrecommit",
			raw:  []byte(`{"_msg":"Added vote to precommit","data":"Votes:3/4(0.750)","height":4,"level":"debug","module":"consensus","round":0,"ts":"2025-06-08T01:24:25.320685Z","validator":"47F2A97347636F3F163AA0A80EBC148AEB00CC64","vote_timestamp":"2025-06-08T01:24:25.245992Z"}`),
			expected: &types.AddedVoteToPrecommit{
				Data:          "Votes:3/4(0.750)",
				Height:        4,
				Module:        "consensus",
				Round:         0,
				Timestamp:     "2025-06-08T01:24:25.320685Z",
				Validator:     "47F2A97347636F3F163AA0A80EBC148AEB00CC64",
				VoteTimestamp: "2025-06-08T01:24:25.245992Z",
				Level:         "debug",
			},
		},
		{
			name: "AddedVoteToLastPrecommits",
			raw:  []byte(`{"_msg":"Added vote to last precommits","last_commit":"VoteSet{H:4 R:0 T:SIGNED_MSG_TYPE_PRECOMMIT +2/3:C3C61C93E975E4F6321BA3ACFBBCA6CB9678959A5B7F932A8D9B21BD5E42AF8C:1:87DFE237E181(1) BA{4:xxxx} map[]}","level":"debug","module":"consensus","ts":"2025-06-08T01:24:25.396405Z"}`),
			expected: &types.AddedVoteToLastPrecommits{
				LastCommit: "VoteSet{H:4 R:0 T:SIGNED_MSG_TYPE_PRECOMMIT +2/3:C3C61C93E975E4F6321BA3ACFBBCA6CB9678959A5B7F932A8D9B21BD5E42AF8C:1:87DFE237E181(1) BA{4:xxxx} map[]}",
				Module:     "consensus",
				Timestamp:  "2025-06-08T01:24:25.396405Z",
				Level:      "debug",
			},
		},
		{
			name: "SendingVoteMessage",
			raw:  []byte(`{"_msg":"Sending vote message","level":"debug","module":"consensus","ps":{"round_state":{"height":"5","round":0,"step":1,"start_time":"2025-06-08T01:24:25.396334Z","proposal":false,"proposal_block_part_set_header":{"total":0,"hash":""},"proposal_block_parts":null,"proposal_pol_round":-1,"proposal_pol":null,"prevotes":null,"precommits":null,"last_commit_round":0,"last_commit":"xxx_","catchup_commit_round":-1,"catchup_commit":null},"stats":{"votes":"8","block_parts":"1"}},"ts":"2025-06-08T01:24:25.403938Z","vote":"Vote{3:47F2A9734763 4/00/SIGNED_MSG_TYPE_PRECOMMIT(Precommit) C3C61C93E975 2F889050988E 000000000000 @ 2025-06-08T01:24:25.245992Z}"}`),
			expected: &types.SendingVoteMessage{
				Module: "consensus",
				PeerState: types.PeerState{
					RoundState: types.RoundState{
						Height:                     "5",
						Round:                      0,
						Step:                       1,
						StartTime:                  time.Date(2025, 6, 8, 1, 24, 25, 396334000, time.UTC),
						Proposal:                   false,
						ProposalBlockPartSetHeader: types.ProposalBlockPartSetHeader{Total: 0, Hash: ""},
						ProposalBlockParts:         nil,
						ProposalPOLRound:           -1,
						ProposalPOL:                nil,
						Prevotes:                   nil,
						Precommits:                 nil,
						LastCommitRound:            0,
						LastCommit:                 "xxx_",
						CatchupCommitRound:         -1,
						CatchupCommit:              nil,
					},
					Stats: types.Stats{
						Votes:      "8",
						BlockParts: "1",
					},
				},
				Timestamp: "2025-06-08T01:24:25.403938Z",
				Vote:      "Vote{3:47F2A9734763 4/00/SIGNED_MSG_TYPE_PRECOMMIT(Precommit) C3C61C93E975 2F889050988E 000000000000 @ 2025-06-08T01:24:25.245992Z}",
				Level:     "debug",
			},
		},
		{
			name: "Send",
			raw:  []byte(`{"_msg":"Send","channel":34,"conn":"MConn{127.0.0.1:57194}","level":"debug","module":"p2p","msgBytes":"32B8010AB5010802100422480A20C3C61C93E975E4F6321BA3ACFBBCA6CB9678959A5B7F932A8D9B21BD5E42AF8C12240801122087DFE237E181CBB6E93EB06A7B60A205BC171E2950116B9ACF19D58FBCAA031B2A0B08C9CC93C20610C094A675321447F2A97347636F3F163AA0A80EBC148AEB00CC64380342402F889050988E20DD97116E9DD84ED0CB065F65C2F9E8B05A5CCCC321BAB7C0C777D106DFD794FE63253D574210BA8CD13B2C2166A6B07FD2F9901F50EBFA110B","peer":"344fddf3df5ad8944fd962ce702bd76392a33eff@127.0.0.1:57194","ts":"2025-06-08T01:24:25.403968Z"}`),
			expected: &types.Send{
				Channel:   34,
				Conn:      "MConn{127.0.0.1:57194}",
				Module:    "p2p",
				MsgBytes:  "32B8010AB5010802100422480A20C3C61C93E975E4F6321BA3ACFBBCA6CB9678959A5B7F932A8D9B21BD5E42AF8C12240801122087DFE237E181CBB6E93EB06A7B60A205BC171E2950116B9ACF19D58FBCAA031B2A0B08C9CC93C20610C094A675321447F2A97347636F3F163AA0A80EBC148AEB00CC64380342402F889050988E20DD97116E9DD84ED0CB065F65C2F9E8B05A5CCCC321BAB7C0C777D106DFD794FE63253D574210BA8CD13B2C2166A6B07FD2F9901F50EBFA110B",
				Peer:      "344fddf3df5ad8944fd962ce702bd76392a33eff@127.0.0.1:57194",
				Timestamp: "2025-06-08T01:24:25.403968Z",
				Level:     "debug",
			},
		},
		{
			name: "Receive",
			raw:  []byte(`{"_msg":"Receive","chId":32,"level":"debug","module":"consensus","msg":"[HasVote VI:1 V:{2/00/SIGNED_MSG_TYPE_PREVOTE}]","src":"Peer{MConn{127.0.0.1:57203} a0b24cc231969d2951288f607cd6671c0e20e921 in}","ts":"2025-06-08T01:24:22.533688Z"}`),
			expected: &types.Receive{
				ChannelID:  32,
				Module:     "consensus",
				Msg:        "[HasVote VI:1 V:{2/00/SIGNED_MSG_TYPE_PREVOTE}]",
				SourcePeer: "Peer{MConn{127.0.0.1:57203} a0b24cc231969d2951288f607cd6671c0e20e921 in}",
				Timestamp:  "2025-06-08T01:24:22.533688Z",
				Level:      "debug",
			},
		},
		{
			name: "Receive",
			raw:  []byte(`{"_msg":"Receive","chId":34,"level":"debug","module":"consensus","msg":"[Vote Vote{0:14723CA68371 1/00/SIGNED_MSG_TYPE_PREVOTE(Prevote) 59E565861577 F786A527D8FA 000000000000 @ 2025-06-08T01:24:20.126787Z}]","src":"Peer{MConn{127.0.0.1:57186} 1d8ff37135f1583255143662b1e1343677c59332 in}","ts":"2025-06-08T01:24:20.219037Z"}`),
			expected: &types.Receive{
				ChannelID:  34,
				Module:     "consensus",
				Msg:        "[Vote Vote{0:14723CA68371 1/00/SIGNED_MSG_TYPE_PREVOTE(Prevote) 59E565861577 F786A527D8FA 000000000000 @ 2025-06-08T01:24:20.126787Z}]",
				SourcePeer: "Peer{MConn{127.0.0.1:57186} 1d8ff37135f1583255143662b1e1343677c59332 in}",
				Timestamp:  "2025-06-08T01:24:20.219037Z",
				Level:      "debug",
			},
		},
		{
			name: "ReadPacketMsg",
			raw:  []byte(`{"_msg":"Read PacketMsg","conn":"MConn{127.0.0.1:57194}","level":"debug","module":"p2p","packet":{"channel_id":32,"eof":true,"data":"OgYIAhgBIAI="},"peer":"344fddf3df5ad8944fd962ce702bd76392a33eff@127.0.0.1:57194","ts":"2025-06-08T01:24:22.537898Z"}`),
			expected: &types.ReadPacketMsg{
				Connection: "MConn{127.0.0.1:57194}",
				Packet: &types.Packet{
					ChannelID: 32,
					Eof:       true,
					Data:      "OgYIAhgBIAI=", // Base64 encoded data
				},
				Module:    "p2p",
				Peer:      "344fddf3df5ad8944fd962ce702bd76392a33eff@127.0.0.1:57194",
				Timestamp: "2025-06-08T01:24:22.537898Z",
				Level:     "debug",
			},
		},
		{
			name: "ReceivedProposal",
			raw:  []byte(`{"_msg":"Received proposal","level":"info","module":"consensus","proposal":"Proposal{1/0 (59E56586157743A042A6975F0320AAEB887A547FF01AF344321ABF8C7B2DE76B:1:711D142694C9, -1) B00C8E3F708F @ 2025-06-07T14:41:31.325889Z}","proposer":"14723CA6837129C61A873631CF7C865AEBBF63B1","ts":"2025-06-08T01:24:20.219083Z"}`),
			expected: &types.ReceivedProposal{
				Module:    "consensus",
				Proposal:  "Proposal{1/0 (59E56586157743A042A6975F0320AAEB887A547FF01AF344321ABF8C7B2DE76B:1:711D142694C9, -1) B00C8E3F708F @ 2025-06-07T14:41:31.325889Z}",
				Proposer:  "14723CA6837129C61A873631CF7C865AEBBF63B1",
				Timestamp: "2025-06-08T01:24:20.219083Z",
				Level:     "info",
			},
		},
		{
			name: "ReceiveBlockPart",
			raw:  []byte(`{"_msg":"Receive block part","count":1,"from":"1d8ff37135f1583255143662b1e1343677c59332","height":1,"index":0,"level":"debug","module":"consensus","round":0,"total":1,"ts":"2025-06-08T01:24:20.219116Z"}`),
			expected: &types.ReceiveBlockPart{
				Count:     1,
				From:      "1d8ff37135f1583255143662b1e1343677c59332",
				Height:    1,
				Index:     0,
				Module:    "consensus",
				Round:     0,
				Total:     1,
				Timestamp: "2025-06-08T01:24:20.219116Z",
				Level:     "debug",
			},
		},
		{
			name: "ReceiveCompleteProposalBlock",
			raw:  []byte(`{"_msg":"Received complete proposal block","hash":"B5B87A0CB4173DB4843C223FDD880E46117D7FD2D45B0FDCA54F5A71A3EAC56F","height":2,"level":"info","module":"consensus","ts":"2025-06-08T01:24:22.439914Z"}`),
			expected: &types.ReceivedCompleteProposalBlock{
				Module:    "consensus",
				Hash:      "B5B87A0CB4173DB4843C223FDD880E46117D7FD2D45B0FDCA54F5A71A3EAC56F",
				Height:    2,
				Timestamp: "2025-06-08T01:24:22.439914Z",
				Level:     "info",
			},
		},
		{
			name: "FinalizingCommitOfBlock",
			raw:  []byte(`{"_msg":"Finalizing commit of block","hash":"59E56586157743A042A6975F0320AAEB887A547FF01AF344321ABF8C7B2DE76B","height":1,"level":"info","module":"consensus","num_txs":0,"root":"0000000000000000","ts":"2025-06-08T01:24:21.424291Z"}`),
			expected: &types.FinalizingCommitOfBlock{
				Hash:      "59E56586157743A042A6975F0320AAEB887A547FF01AF344321ABF8C7B2DE76B",
				Height:    1,
				NumTxs:    0,
				Root:      "0000000000000000",
				Module:    "consensus",
				Timestamp: "2025-06-08T01:24:21.424291Z",
				Level:     "info",
			},
		},
		{
			name: "CommittedBlock",
			raw:  []byte(`{"_msg":"Committed block","block":"Block{\n  Header{\n    Version:        {11 1}\n    ChainID:        chain-QNZbFN\n    Height:         1\n    SentTime:           2025-06-07 14:41:31.325889 +0000 UTC\n    LastBlockID:    :0:000000000000\n    LastCommit:     E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855\n    Data:           E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855\n    Validators:     EAC48B92B84F5B99FE50813111A7BEB72EC701C9A3682ED2F308D3EDF6A2126D\n    NextValidators: EAC48B92B84F5B99FE50813111A7BEB72EC701C9A3682ED2F308D3EDF6A2126D\n    App:            0000000000000000\n    Consensus:      68ECD6F333119CE43751ECE583B981F23508AEAF4221FF582B1BB33BE42BCEFA\n    Results:        E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855\n    Evidence:       E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855\n    Proposer:       14723CA6837129C61A873631CF7C865AEBBF63B1\n  }#59E56586157743A042A6975F0320AAEB887A547FF01AF344321ABF8C7B2DE76B\n  Data{\n    \n  }#E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855\n  EvidenceData{\n    \n  }#E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855\n  Commit{\n    Height:     0\n    Round:      0\n    BlockID:    :0:000000000000\n    Signatures:\n      \n  }#E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855\n}#59E56586157743A042A6975F0320AAEB887A547FF01AF344321ABF8C7B2DE76B","height":1,"level":"debug","module":"consensus","ts":"2025-06-08T01:24:21.424309Z"}`),
			expected: &types.CommittedBlock{
				Block:     "Block{\n  Header{\n    Version:        {11 1}\n    ChainID:        chain-QNZbFN\n    Height:         1\n    SentTime:           2025-06-07 14:41:31.325889 +0000 UTC\n    LastBlockID:    :0:000000000000\n    LastCommit:     E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855\n    Data:           E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855\n    Validators:     EAC48B92B84F5B99FE50813111A7BEB72EC701C9A3682ED2F308D3EDF6A2126D\n    NextValidators: EAC48B92B84F5B99FE50813111A7BEB72EC701C9A3682ED2F308D3EDF6A2126D\n    App:            0000000000000000\n    Consensus:      68ECD6F333119CE43751ECE583B981F23508AEAF4221FF582B1BB33BE42BCEFA\n    Results:        E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855\n    Evidence:       E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855\n    Proposer:       14723CA6837129C61A873631CF7C865AEBBF63B1\n  }#59E56586157743A042A6975F0320AAEB887A547FF01AF344321ABF8C7B2DE76B\n  Data{\n    \n  }#E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855\n  EvidenceData{\n    \n  }#E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855\n  Commit{\n    Height:     0\n    Round:      0\n    BlockID:    :0:000000000000\n    Signatures:\n      \n  }#E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855\n}#59E56586157743A042A6975F0320AAEB887A547FF01AF344321ABF8C7B2DE76B",
				Height:    1,
				Module:    "consensus",
				Timestamp: "2025-06-08T01:24:21.424309Z",
				Level:     "debug",
			},
		},
		{
			name: "UpdatingValidBlock",
			raw:  []byte(`{"_msg":"Updating valid block because of POL","level":"debug","module":"consensus","pol_round":0,"ts":"2025-06-08T01:24:22.620016Z","valid_round":-1}`),
			expected: &types.UpdatingValidBlock{
				PolRound:  0,
				Timestamp: "2025-06-08T01:24:22.620016Z",
				Module:    "consensus",
				Level:     "debug",
			},
		},
		{
			name: "ScheduledTimeout",
			raw:  []byte(`{"_msg":"Scheduled timeout","dur":"3s","height":2,"level":"debug","module":"consensus","round":0,"step":"RoundStepPropose","ts":"2025-06-08T01:24:22.425497Z"}`),
			expected: &types.ScheduledTimeout{
				Duration:  "3s",
				Height:    2,
				Step:      "RoundStepPropose",
				Timestamp: "2025-06-08T01:24:22.425497Z",
				Module:    "consensus",
				Level:     "debug",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Dispatch(tt.raw)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}
