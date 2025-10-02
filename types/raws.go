package types

import (
	"time"
)

type Validator struct {
	Address   string `json:"addr"`
	PubKey    string `json:"pubKey"` // Base64 encoded public key
	Timestamp string `json:"ts"`
	Module    string `json:"module"`
	Level     string `json:"level"` // e.g., "info", "debug", "error"
}

type P2pNodeID struct {
	ID        string `json:"ID"`   // Base64 encoded node ID
	File      string `json:"file"` // Path to the file containing the node ID
	Timestamp string `json:"ts"`
	Module    string `json:"module"`
	Level     string `json:"level"` // e.g., "info", "debug", "error"
}

type EnteringNewRound struct {
	Current   string `json:"current"`
	Previous  string `json:"previous"`
	Proposer  string `json:"proposer"`
	Height    uint64 `json:"height"`
	Round     uint64 `json:"round"`
	Timestamp string `json:"ts"`
	Module    string `json:"module"`
	Level     string `json:"level"` // e.g., "info", "debug", "error"
}

type EnteringNewStep struct {
	Current    string `json:"current"`
	TargetStep string `json:"targetStep"`
	Height     uint64 `json:"height"`
	Round      uint64 `json:"round"`
	Timestamp  string `json:"ts"`
	Module     string `json:"module"`
	Level      string `json:"level"` // e.g., "info", "debug", "error"
}

type ProposeStep struct {
	Height    uint64 `json:"height"`
	Round     uint64 `json:"round"`
	Proposer  string `json:"proposer"`
	Timestamp string `json:"ts"`
	Module    string `json:"module"`
	Level     string `json:"level"` // e.g., "info", "debug", "error"
}

type ProposeStepOurTurn struct {
	Height    uint64 `json:"height"`
	Round     uint64 `json:"round"`
	Proposer  string `json:"proposer"`
	Timestamp string `json:"ts"`
	Module    string `json:"module"`
	Level     string `json:"level"` // e.g., "info", "debug", "error"
}

type ProposeStepNotOurTurn struct {
	Height    uint64 `json:"height"`
	Round     uint64 `json:"round"`
	Proposer  string `json:"proposer"`
	Timestamp string `json:"ts"`
	Module    string `json:"module"`
	Level     string `json:"level"` // e.g., "info", "debug", "error"
}

type LockingBlock struct {
	Hash      string `json:"hash"`
	Height    uint64 `json:"height"`
	Round     uint64 `json:"round"`
	Timestamp string `json:"ts"`
	Module    string `json:"module"`
	Level     string `json:"level"` // e.g., "info", "debug", "error"
}

type AddingVote struct {
	ConsensusHeight              uint64 `json:"cs_height"`
	VoteExtensionLength          uint64 `json:"extLen"`
	VoteExtensionSignatureLength uint64 `json:"extSigLen"`
	ValidatorIndex               uint64 `json:"val_index"`
	VoteHeight                   uint64 `json:"vote_height"`
	VoteType                     string `json:"vote_type"`
	Timestamp                    string `json:"ts"`
	Module                       string `json:"module"`
	Level                        string `json:"level"` // e.g., "info", "debug", "error"
}

type AddedVoteToPrevote struct {
	Vote      string `json:"vote"`
	Prevotes  string `json:"prevotes"`
	Timestamp string `json:"ts"`
	Module    string `json:"module"`
	Level     string `json:"level"`
}

type AddedVoteToPrecommit struct {
	Data          string `json:"data"`
	Height        uint64 `json:"height"`
	Round         uint64 `json:"round"`
	Validator     string `json:"validator"`
	VoteTimestamp string `json:"vote_timestamp"`
	Timestamp     string `json:"ts"`
	Module        string `json:"module"`
	Level         string `json:"level"` // e.g., "info", "debug", "error"
}

type AddedVoteToLastPrecommits struct {
	LastCommit string `json:"last_commit"`
	Timestamp  string `json:"ts"`
	Module     string `json:"module"`
	Level      string `json:"level"` // e.g., "info", "debug", "error"
}

type Receive struct {
	ChannelID  uint64 `json:"chId"`
	Msg        string `json:"msg"`
	SourcePeer string `json:"src"` // empty means self
	Timestamp  string `json:"ts"`
	Module     string `json:"module"`
	Level      string `json:"level"` // e.g., "info", "debug", "error"
}

// ReceivedBytes corresponds to p2p layer "Received bytes" log
type ReceivedBytes struct {
	ChannelID uint64 `json:"chID"`
	MsgBytes  string `json:"msgBytes"` // Base64 encoded
	Peer      string `json:"peer"`
	Timestamp string `json:"ts"`
	Module    string `json:"module"`
	Level     string `json:"level"`
}

type ReadPacketMsg struct {
	Connection string  `json:"conn"`
	Packet     *Packet `json:"packet"`
	Peer       string  `json:"peer"` // empty means self
	Timestamp  string  `json:"ts"`
	Module     string  `json:"module"`
	Level      string  `json:"level"` // e.g., "info", "debug", "error"
}

type Packet struct {
	ChannelID uint64 `json:"channel_id"`
	Eof       bool   `json:"eof"`
	Data      string `json:"data"` // Base64 encoded data
}

type SendingVoteMessage struct {
	PeerState PeerState `json:"ps"`
	Vote      string    `json:"vote"`
	Timestamp string    `json:"ts"`
	Module    string    `json:"module"`
	Level     string    `json:"level"` // e.g., "info", "debug", "error"
}

type PeerState struct {
	RoundState RoundState `json:"round_state"`
	Stats      Stats      `json:"stats"`
}

type RoundState struct {
	Height                     string                     `json:"height"`
	Round                      int                        `json:"round"`
	Step                       int                        `json:"step"`
	StartTime                  time.Time                  `json:"start_time"`
	Proposal                   bool                       `json:"proposal"`
	ProposalBlockPartSetHeader ProposalBlockPartSetHeader `json:"proposal_block_part_set_header"`
	ProposalBlockParts         *string                    `json:"proposal_block_parts"`
	ProposalPOLRound           int                        `json:"proposal_pol_round"`
	ProposalPOL                *string                    `json:"proposal_pol"`
	Prevotes                   *string                    `json:"prevotes"`
	Precommits                 *string                    `json:"precommits"`
	LastCommitRound            int                        `json:"last_commit_round"`
	LastCommit                 string                     `json:"last_commit"`
	CatchupCommitRound         int                        `json:"catchup_commit_round"`
	CatchupCommit              *string                    `json:"catchup_commit"`
}

type ProposalBlockPartSetHeader struct {
	Total int    `json:"total"`
	Hash  string `json:"hash"`
}

type Stats struct {
	Votes      string `json:"votes"`
	BlockParts string `json:"block_parts"`
}

type Send struct {
	Channel   uint64 `json:"channel"`
	Conn      string `json:"conn"`
	MsgBytes  string `json:"msgBytes"`
	Peer      string `json:"peer"`
	Timestamp string `json:"ts"`
	Module    string `json:"module"`
	Level     string `json:"level"` // e.g., "info", "debug", "error"
}

type TrySend struct {
	Channel   uint64 `json:"channel"`
	Conn      string `json:"conn"`
	MsgBytes  string `json:"msgBytes"`
	Peer      string `json:"peer"`
	Timestamp string `json:"ts"`
	Module    string `json:"module"`
	Level     string `json:"level"`
}

type ReceivedProposal struct {
	Proposal  string `json:"proposal"`
	Proposer  string `json:"proposer"`
	Timestamp string `json:"ts"`
	Module    string `json:"module"`
	Level     string `json:"level"` // e.g., "info", "debug", "error"
}

type ReceiveBlockPart struct {
	Count     uint64 `json:"count"`
	From      string `json:"from"` // empty means self
	Height    uint64 `json:"height"`
	Index     uint64 `json:"index"`
	Round     uint64 `json:"round"`
	Total     uint64 `json:"total"`
	Timestamp string `json:"ts"`
	Module    string `json:"module"`
	Level     string `json:"level"` // e.g., "info", "debug", "error"
}

type ReceivedCompleteProposalBlock struct {
	Hash      string `json:"hash"`
	Height    uint64 `json:"height"`
	Timestamp string `json:"ts"`
	Module    string `json:"module"`
	Level     string `json:"level"` // e.g., "info", "debug", "error"
}

type ScheduledTimeout struct {
	Duration  string `json:"dur"`
	Height    uint64 `json:"height"`
	Round     uint64 `json:"round"`
	Step      string `json:"step"`
	Timestamp string `json:"ts"`
	Module    string `json:"module"`
	Level     string `json:"level"` // e.g., "info", "debug", "error"
}

type SignedProposal struct {
	Height    uint64 `json:"height"`
	Round     uint64 `json:"round"`
	Proposal  string `json:"proposal"` // Base64 encoded proposal
	Timestamp string `json:"ts"`
	Module    string `json:"module"`
	Level     string `json:"level"` // e.g., "info", "debug", "error"
}

type FinalizingCommitOfBlock struct {
	Hash      string `json:"hash"`
	Height    uint64 `json:"height"`
	NumTxs    uint64 `json:"num_txs"`
	Root      string `json:"root"`
	Timestamp string `json:"ts"`
	Module    string `json:"module"`
	Level     string `json:"level"` // e.g., "info", "debug", "error"
}

type CommittedBlock struct {
	Block     string `json:"block"`
	Height    uint64 `json:"height"`
	Timestamp string `json:"ts"`
	Module    string `json:"module"`
	Level     string `json:"level"` // e.g., "info", "debug", "error"
}

type UpdatingValidBlock struct {
	PolRound  int    `json:"pol_round"`
	Timestamp string `json:"ts"`
	Module    string `json:"module"`
	Level     string `json:"level"` // e.g., "info", "debug", "error"
}
