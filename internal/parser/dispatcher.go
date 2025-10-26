package parser

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/bft-labs/cometbft-log-etl/types"
	"io"
	"strings"
)

// ParserFunc is a function that takes raw JSON and returns a parsed event.
type ParserFunc func(raw []byte) (interface{}, error)

// dispatchMap maps each _msg constant to its ParserFunc.
var dispatchMap = map[string]ParserFunc{
	types.MsgValidator:                     parseValidator,
	types.MsgP2pNodeID:                     parseP2pNodeID,
	types.MsgSignedProposal:                parseSignedProposal,
	types.MsgAddingVote:                    parseAddingVote,
	types.MsgAddedVoteToPrevote:            parseAddedVoteToPrevote,
	types.MsgAddedVoteToPrecommit:          parseAddedVoteToPrecommit,
	types.MsgAddedVoteToLastPrecommits:     parseAddedVoteToLastPrecommits,
	types.MsgSendingVoteMessage:            parseSendingVoteMessage,
	types.MsgSend:                          parseSend,
	types.MsgTrySend:                       parseTrySend,
	types.MsgReceive:                       parseReceive,
	types.MsgReadPacketMsg:                 parseReadPacketMsg,
	types.MsgReceivedBytes:                 parseReceivedBytes,
	types.MsgReceivedProposal:              parseReceivedProposal,
	types.MsgReceiveBlockPart:              parseReceiveBlockPart,
	types.MsgReceivedCompleteProposalBlock: parseReceivedCompleteProposalBlock,

	types.MsgEnteringNewRound:                         parseEnteringNewRound,
	types.MsgEnteringNewRoundWithInvalidArgs:          parseEnteringNewRound,
	types.MsgEnteringProposeStep:                      parseEnteringNewStep,
	types.MsgEnteringProposeStepWithInvalidArgs:       parseEnteringNewStep,
	types.MsgEnteringPrevoteStep:                      parseEnteringNewStep,
	types.MsgEnteringPrevoteStepWithInvalidArgs:       parseEnteringNewStep,
	types.MsgEnteringPrevoteWaitStep:                  parseEnteringNewStep,
	types.MsgEnteringPrevoteWaitStepWithInvalidArgs:   parseEnteringNewStep,
	types.MsgEnteringPrecommitStep:                    parseEnteringNewStep,
	types.MsgEnteringPrecommitStepWithInvalidArgs:     parseEnteringNewStep,
	types.MsgEnteringPrecommitWaitStep:                parseEnteringNewStep,
	types.MsgEnteringPrecommitWaitStepWithInvalidArgs: parseEnteringNewStep,
	types.MsgEnteringCommitStep:                       parseEnteringNewStep,
	types.MsgEnteringCommitStepWithInvalidArgs:        parseEnteringNewStep,
	types.MsgLockingBlock:                             parseLockingBlock,
	types.MsgProposeStepTurnToPropose:                 parseProposeStepOurTurn,
	types.MsgProposeStepNotTurnToPropose:              parseProposeStepNotOurTurn,
	types.MsgScheduledTimeout:                         parseScheduledTimeout,
	types.MsgFinalizingCommitOfBlock:                  parseFinalizingCommitOfBlock,
	types.MsgCommittedBlock:                           parseCommittedBlock,
	types.MsgUpdatingValidBlock:                       parseUpdatingValidBlock,
}

// dispatchMapCI is a case-insensitive lookup map for `_msg` keys.
// Keys are lowercased; values are the same ParserFunc as dispatchMap.
var dispatchMapCI map[string]ParserFunc

func init() {
	dispatchMapCI = make(map[string]ParserFunc, len(dispatchMap))
	for k, v := range dispatchMap {
		dispatchMapCI[strings.ToLower(k)] = v
	}
}

// Dispatch inspects the raw JSON blob, extracts `_msg`, and calls the right parser.
func Dispatch(raw []byte) (interface{}, error) {
	// 1) Peek at the `_msg` field
	var header struct {
		Msg string `json:"_msg"`
	}
	if err := json.Unmarshal(raw, &header); err != nil {
		// Every log entry doesn't have a `_msg` field, so failing to find it is not an error.
		return nil, nil
	}

	// 1.5) Skip any consensus step entries logged "with invalid args"
	// These are spurious transitions and should not produce events.
	if msg := strings.ToLower(header.Msg); strings.Contains(msg, "with invalid args") {
		return nil, nil
	}

	// 2) Find the parser function (case-insensitive on `_msg`)
	// Prefer case-insensitive lookup to tolerate variations like
	// "entering propose step" vs "Entering propose step".
	parserFn, ok := dispatchMapCI[strings.ToLower(header.Msg)]
	if !ok {
		// We don't have a parser for every message type, this is intentionally left as nil.
		// fmt.Printf("No parser for message type: %s\n", header.Msg) // Uncomment for debugging
		return nil, nil
	}

	// 3) Delegate to the specific parser
	return parserFn(raw)
}

// ParseStream reads newline-delimited JSON logs from r, parses each entry,
// and sends the typed result into the out channel.
func ParseStream(r io.Reader, out chan<- interface{}) error {
	scanner := bufio.NewScanner(r)
	// Increase buffer to handle very long log lines (default is 64K).
	// Allocate a 1MB initial buffer with a generous 64MB max token size.
	buf := make([]byte, 0, 1<<20)
	scanner.Buffer(buf, 64<<20)
	for scanner.Scan() {
		raw := make([]byte, len(scanner.Bytes()))
		copy(raw, scanner.Bytes())

		evt, err := Dispatch(raw)
		if err != nil {
			return fmt.Errorf("parse error: %w", err)
		}
		out <- evt
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan error: %w", err)
	}
	return nil
}
