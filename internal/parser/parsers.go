// pkg/parser/parsers.go
package parser

import (
	"encoding/json"
	"fmt"
	"github.com/bft-labs/cometbft-log-etl/types"
	"strings"
)

func parseValidator(raw []byte) (interface{}, error) {
	var ev types.Validator
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("unmarshal Validator: %w", err)
	}
	return &ev, nil
}

func parseP2pNodeID(raw []byte) (interface{}, error) {
	var ev types.P2pNodeID
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("unmarshal P2pNodeID: %w", err)
	}
	return &ev, nil
}

func parseProposeStepOurTurn(raw []byte) (interface{}, error) {
	var ev types.ProposeStepOurTurn
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("unmarshal ProposeStepOurTurn: %w", err)
	}
	return &ev, nil
}

func parseProposeStepNotOurTurn(raw []byte) (interface{}, error) {
	var ev types.ProposeStepNotOurTurn
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("unmarshal ProposeStepNotOurTurn: %w", err)
	}
	return &ev, nil
}

func parseScheduledTimeout(raw []byte) (interface{}, error) {
	var ev types.ScheduledTimeout
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("unmarshal ScheduledTimeout: %w", err)
	}
	return &ev, nil
}

func parseSignedProposal(raw []byte) (interface{}, error) {
	var ev types.SignedProposal
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("unmarshal SignedProposal: %w", err)
	}
	return &ev, nil
}

// parseAddedVoteToPrevote unmarshals into types.AddedVoteToPrevote
func parseAddedVoteToPrevote(raw []byte) (interface{}, error) {
	var ev types.AddedVoteToPrevote
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("unmarshal AddedVoteToPrevote: %w", err)
	}
	return &ev, nil
}

// parseAddedVoteToPrecommit unmarshals into types.AddedVoteToPrecommit
func parseAddedVoteToPrecommit(raw []byte) (interface{}, error) {
	var ev types.AddedVoteToPrecommit
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("unmarshal AddedVoteToPrecommit: %w", err)
	}
	return &ev, nil
}

// parseAddedVoteToLastPrecommits unmarshals into types.AddedVoteToLastPrecommits
func parseAddedVoteToLastPrecommits(raw []byte) (interface{}, error) {
	var ev types.AddedVoteToLastPrecommits
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("unmarshal AddedVoteToLastPrecommits: %w", err)
	}
	return &ev, nil
}

func parseEnteringNewRound(raw []byte) (interface{}, error) {
	var ev types.EnteringNewRound
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("unmarshal EnteringNewRound: %w", err)
	}
	return &ev, nil
}

func parseEnteringNewStep(raw []byte) (interface{}, error) {
	var header struct {
		Msg string `json:"_msg"`
	}
	if err := json.Unmarshal(raw, &header); err != nil {
		return nil, fmt.Errorf("unmarshal header: %w", err)
	}

	targetSteps := []string{
		"propose", "prevote", "prevote_wait",
		"precommit", "precommit_wait", "commit",
	}

	var targetStep string
	for _, step := range targetSteps {
		if strings.Contains(header.Msg, step) {
			targetStep = step
			break
		}
	}

	var ev types.EnteringNewStep
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("unmarshal EnteringNewStep: %w", err)
	}
	ev.TargetStep = targetStep
	return &ev, nil
}

func parseLockingBlock(raw []byte) (interface{}, error) {
	var ev types.LockingBlock
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("unmarshal LockingBlock: %w", err)
	}
	return &ev, nil
}

func parseAddingVote(raw []byte) (interface{}, error) {
	var ev types.AddingVote
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("unmarshal AddingVote: %w", err)
	}
	return &ev, nil
}

func parseSendingVoteMessage(raw []byte) (interface{}, error) {
	var ev types.SendingVoteMessage
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("unmarshal SendingVoteMessage: %w", err)
	}
	return &ev, nil
}

func parseSend(raw []byte) (interface{}, error) {
	var ev types.Send
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("unmarshal Send: %w", err)
	}
	return &ev, nil
}

func parseTrySend(raw []byte) (interface{}, error) {
	var ev types.TrySend
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("unmarshal TrySend: %w", err)
	}
	return &ev, nil
}

func parseReceivedProposal(raw []byte) (interface{}, error) {
	var ev types.ReceivedProposal
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("unmarshal ReceivedProposal: %w", err)
	}

	return &ev, nil
}

func parseReceiveBlockPart(raw []byte) (interface{}, error) {
	var ev types.ReceiveBlockPart
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("unmarshal ReceiveBlockPart: %w", err)
	}
	return &ev, nil
}

func parseReceivedCompleteProposalBlock(raw []byte) (interface{}, error) {
	var ev types.ReceivedCompleteProposalBlock
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("unmarshal ReceivedCompleteProposalBlock: %w", err)
	}
	return &ev, nil
}

func parseReceive(raw []byte) (interface{}, error) {
	var ev types.Receive
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("unmarshal Receive: %w", err)
	}
	return &ev, nil
}

func parseReceivedBytes(raw []byte) (interface{}, error) {
	var ev types.ReceivedBytes
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("unmarshal ReceivedBytes: %w", err)
	}
	return &ev, nil
}

func parseReadPacketMsg(raw []byte) (interface{}, error) {
	var ev types.ReadPacketMsg
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("unmarshal ReadPacketMsg: %w", err)
	}
	return &ev, nil
}

func parseFinalizingCommitOfBlock(raw []byte) (interface{}, error) {
	var ev types.FinalizingCommitOfBlock
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("unmarshal FinalizingCommitOfBlock: %w", err)
	}
	return &ev, nil
}

func parseCommittedBlock(raw []byte) (interface{}, error) {
	var ev types.CommittedBlock
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("unmarshal CommittedBlock: %w", err)
	}
	return &ev, nil
}

func parseUpdatingValidBlock(raw []byte) (interface{}, error) {
	var ev types.UpdatingValidBlock
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("unmarshal UpdatingValidBlock: %w", err)
	}
	return &ev, nil
}
