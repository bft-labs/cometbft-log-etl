package types

// Channel constants as defined in CometBFT
const (
	PexChannel         = 0x00 // Peer exchange channel
	BlocksyncChannel   = 0x40 // Block sync channel
	VoteSetBitsChannel = 0x23 // Vote set bits channel
	EvidenceChannel    = 0x38 // Evidence channel
	MempoolChannel     = 0x30 // Mempool channel
	SnapshotChannel    = 0x60 // Snapshot channel
	ChunkChannel       = 0x61 // Chunk channel
	DataChannel        = 0x21 // Data channel (consensus)
	VoteChannel        = 0x22 // Vote channel (consensus)
	StateChannel       = 0x20 // State channel (consensus)
)

// ChannelNames maps channel IDs to human-readable names
var ChannelNames = map[uint64]string{
	PexChannel:         "pex",
	BlocksyncChannel:   "blocksync",
	VoteSetBitsChannel: "vote_set_bits",
	EvidenceChannel:    "evidence",
	MempoolChannel:     "mempool",
	SnapshotChannel:    "snapshot",
	ChunkChannel:       "chunk",
	DataChannel:        "data",
	VoteChannel:        "vote",
	StateChannel:       "state",
}

// IsConsensusChannel returns true if the channel is related to consensus
func IsConsensusChannel(channelID uint64) bool {
	switch channelID {
	case DataChannel, VoteChannel, StateChannel, VoteSetBitsChannel:
		return true
	default:
		return false
	}
}

// GetChannelName returns the human-readable name for a channel ID
func GetChannelName(channelID uint64) string {
	if name, exists := ChannelNames[channelID]; exists {
		return name
	}
	return "unknown"
}
