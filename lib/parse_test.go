package lib

import (
	"crypto/rand"
	"fmt"
	"github.com/bft-labs/cometbft-analyzer-types/pkg/core"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestParseVoteString(t *testing.T) {
	input := "[Vote Vote{0:14723CA68371 1/00/SIGNED_MSG_TYPE_PREVOTE(Prevote) 59E565861577 F786A527D8FA 000000000000 @ 2025-06-08T01:24:20.126787Z}]"
	expectedResult := core.Vote{
		Type:   "prevote",
		Height: 1,
		Round:  0,
		BlockId: core.BlockID{
			Hash: "59E565861577",
			PartSetHeader: core.PartSetHeader{
				Total: 0,
				Hash:  "F786A527D8FA",
			},
		},
		Timestamp:        MustParseUtcTimestamp("2025-06-08T01:24:20.126787Z"),
		ValidatorAddress: "14723CA68371",
		ValidatorIndex:   0,
		Signature:        "",
		Extension:        "",
	}

	result, err := ParseVoteString(input)
	require.NoError(t, err, "ParseVoteString should not return an error")
	require.Equal(t, &expectedResult, result, "Parsed vote does not match expected result")
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

func randHex(n int) string {
	return fmt.Sprintf("%X", randBytes(n))
}

func TestBlockFromString(t *testing.T) {
	block := makeTestBlock()
	blockStr := block.String()

	parsedBlock, err := ParseBlockString(blockStr)
	if err != nil {
		t.Fatalf("ParseBlockString failed: %v", err)
	}
	if parsedBlock == nil {
		t.Fatal("Expected non-nil block")
	}

	if block.Header.Height != parsedBlock.Header.Height {
		t.Errorf("Height mismatch: expected %d, got %d", block.Header.Height, parsedBlock.Header.Height)
	}
	if block.Header.ChainID != parsedBlock.Header.ChainID {
		t.Errorf("ChainID mismatch: expected %s, got %s", block.Header.ChainID, parsedBlock.Header.ChainID)
	}
	if block.Header.Version.Block != parsedBlock.Header.Version.Block {
		t.Errorf("Version.Block mismatch: expected %d, got %d", block.Header.Version.Block, parsedBlock.Header.Version.Block)
	}
	if block.Header.Version.App != parsedBlock.Header.Version.App {
		t.Errorf("Version.App mismatch: expected %d, got %d", block.Header.Version.App, parsedBlock.Header.Version.App)
	}

	if block.Header.LastCommitHash != parsedBlock.Header.LastCommitHash {
		t.Errorf("LastCommitHash mismatch: expected %s, got %s", block.Header.LastCommitHash, parsedBlock.Header.LastCommitHash)
	}
	if block.Header.DataHash != parsedBlock.Header.DataHash {
		t.Errorf("DataHash mismatch: expected %s, got %s", block.Header.DataHash, parsedBlock.Header.DataHash)
	}
	if block.Header.ValidatorsHash != parsedBlock.Header.ValidatorsHash {
		t.Errorf("ValidatorsHash mismatch: expected %s, got %s", block.Header.ValidatorsHash, parsedBlock.Header.ValidatorsHash)
	}
	if block.Header.NextValidatorsHash != parsedBlock.Header.NextValidatorsHash {
		t.Errorf("NextValidatorsHash mismatch: expected %s, got %s", block.Header.NextValidatorsHash, parsedBlock.Header.NextValidatorsHash)
	}
	if block.Header.ConsensusHash != parsedBlock.Header.ConsensusHash {
		t.Errorf("ConsensusHash mismatch: expected %s, got %s", block.Header.ConsensusHash, parsedBlock.Header.ConsensusHash)
	}
	if block.Header.AppHash != parsedBlock.Header.AppHash {
		t.Errorf("AppHash mismatch: expected %s, got %s", block.Header.AppHash, parsedBlock.Header.AppHash)
	}
	if block.Header.LastResultsHash != parsedBlock.Header.LastResultsHash {
		t.Errorf("LastResultsHash mismatch: expected %s, got %s", block.Header.LastResultsHash, parsedBlock.Header.LastResultsHash)
	}
	if block.Header.EvidenceHash != parsedBlock.Header.EvidenceHash {
		t.Errorf("EvidenceHash mismatch: expected %s, got %s", block.Header.EvidenceHash, parsedBlock.Header.EvidenceHash)
	}
	if block.Header.ProposerAddress != parsedBlock.Header.ProposerAddress {
		t.Errorf("ProposerAddress mismatch: expected %s, got %s", block.Header.ProposerAddress, parsedBlock.Header.ProposerAddress)
	}

	if block.Header.LastBlockID.Hash != parsedBlock.Header.LastBlockID.Hash {
		t.Errorf("LastBlockID.Hash mismatch: expected %s, got %s", block.Header.LastBlockID.Hash, parsedBlock.Header.LastBlockID.Hash)
	}
	if block.Header.LastBlockID.PartSetHeader.Total != parsedBlock.Header.LastBlockID.PartSetHeader.Total {
		t.Errorf("LastBlockID.PartSetHeader.Total mismatch: expected %d, got %d", block.Header.LastBlockID.PartSetHeader.Total, parsedBlock.Header.LastBlockID.PartSetHeader.Total)
	}

	if block.LastCommit != nil {
		if block.LastCommit.Height != parsedBlock.LastCommit.Height {
			t.Errorf("LastCommit.Height mismatch: expected %d, got %d", block.LastCommit.Height, parsedBlock.LastCommit.Height)
		}
		if block.LastCommit.Round != parsedBlock.LastCommit.Round {
			t.Errorf("LastCommit.Round mismatch: expected %d, got %d", block.LastCommit.Round, parsedBlock.LastCommit.Round)
		}
		if block.LastCommit.BlockID.Hash != parsedBlock.LastCommit.BlockID.Hash {
			t.Errorf("LastCommit.BlockID.Hash mismatch: expected %s, got %s", block.LastCommit.BlockID.Hash, parsedBlock.LastCommit.BlockID.Hash)
		}
		if block.LastCommit.BlockID.PartSetHeader.Total != parsedBlock.LastCommit.BlockID.PartSetHeader.Total {
			t.Errorf("LastCommit.BlockID.PartSetHeader.Total mismatch: expected %d, got %d", block.LastCommit.BlockID.PartSetHeader.Total, parsedBlock.LastCommit.BlockID.PartSetHeader.Total)
		}
	}
}

func TestBlockFromString_NilBlock(t *testing.T) {
	block, err := ParseBlockString("nil-Block")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if block != nil {
		t.Fatal("Expected nil block")
	}
}

func TestBlockFromString_InvalidFormat(t *testing.T) {
	testCases := []struct {
		name  string
		input string
	}{
		{"Empty string", ""},
		{"Invalid start", "NotABlock{}"},
		{"Missing header", "Block{\n}"},
		{"Malformed header", "Block{\n  Header{\n    Invalid: field\n  }\n}"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseBlockString(tc.input)
			if err == nil {
				t.Error("Expected error but got none")
			}
		})
	}
}

func TestParseConsensusVersion(t *testing.T) {
	testCases := []struct {
		input    string
		expected core.Consensus
		hasError bool
	}{
		{
			input:    "{11 0}",
			expected: core.Consensus{Block: 11, App: 0},
			hasError: false,
		},
		{
			input:    "{Block:11 App:0}",
			expected: core.Consensus{Block: 11, App: 0},
			hasError: false,
		},
		{
			input:    "{1 2}",
			expected: core.Consensus{Block: 1, App: 2},
			hasError: false,
		},
		{
			input:    "invalid",
			expected: core.Consensus{},
			hasError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("input_%s", tc.input), func(t *testing.T) {
			result, err := parseConsensusVersion(tc.input)
			if tc.hasError {
				if err == nil {
					t.Error("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if result.Block != tc.expected.Block {
					t.Errorf("Block mismatch: expected %d, got %d", tc.expected.Block, result.Block)
				}
				if result.App != tc.expected.App {
					t.Errorf("App mismatch: expected %d, got %d", tc.expected.App, result.App)
				}
			}
		})
	}
}

func TestParseBlockID(t *testing.T) {
	hash := randHex(32)
	partHash := randHex(32)

	testCases := []struct {
		name     string
		input    string
		expected core.BlockID
		hasError bool
	}{
		{
			name:  "Valid BlockID with PartSetHeader",
			input: fmt.Sprintf("%s:100:%s", hash, partHash),
			expected: core.BlockID{
				Hash: hash,
				PartSetHeader: core.PartSetHeader{
					Total: 100,
					Hash:  partHash,
				},
			},
			hasError: false,
		},
		{
			name:  "BlockID without PartSetHeader",
			input: fmt.Sprintf("%s:", hash),
			expected: core.BlockID{
				Hash: hash,
			},
			hasError: false,
		},
		{
			name:     "Invalid format",
			input:    "invalid",
			expected: core.BlockID{},
			hasError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := parseBlockID(tc.input)
			if tc.hasError {
				if err == nil {
					t.Error("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if result.Hash != tc.expected.Hash {
					t.Errorf("Hash mismatch: expected %s, got %s", tc.expected.Hash, result.Hash)
				}
				if result.PartSetHeader.Total != tc.expected.PartSetHeader.Total {
					t.Errorf("PartSetHeader.Total mismatch: expected %d, got %d", tc.expected.PartSetHeader.Total, result.PartSetHeader.Total)
				}
				if result.PartSetHeader.Hash != tc.expected.PartSetHeader.Hash {
					t.Errorf("PartSetHeader.Hash mismatch: expected %s, got %s", tc.expected.PartSetHeader.Hash, result.PartSetHeader.Hash)
				}
			}
		})
	}
}

func TestParseCommitSig(t *testing.T) {
	sig := randHex(20)
	addr := randHex(20)
	timestamp := time.Now().UTC().Round(time.Second)

	testCases := []struct {
		name     string
		input    string
		expected core.CommitSig
		hasError bool
	}{
		{
			name:  "Valid CommitSig",
			input: fmt.Sprintf("CommitSig{%s by %s on 2 @ %s}", sig, addr, timestamp.Format(time.RFC3339)),
			expected: core.CommitSig{
				BlockIDFlag:      core.BlockIDFlagCommit,
				ValidatorAddress: addr,
				Timestamp:        timestamp,
				Signature:        sig,
			},
			hasError: false,
		},
		{
			name:     "Absent CommitSig",
			input:    "CommitSig{nil-CommitSig}",
			expected: core.NewCommitSigAbsent(),
			hasError: false,
		},
		{
			name:     "Invalid format",
			input:    "NotACommitSig",
			expected: core.CommitSig{},
			hasError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := parseCommitSig(tc.input)
			if tc.hasError {
				if err == nil {
					t.Error("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if result.BlockIDFlag != tc.expected.BlockIDFlag {
					t.Errorf("BlockIDFlag mismatch: expected %v, got %v", tc.expected.BlockIDFlag, result.BlockIDFlag)
				}
				if result.ValidatorAddress != tc.expected.ValidatorAddress {
					t.Errorf("ValidatorAddress mismatch: expected %s, got %s", tc.expected.ValidatorAddress, result.ValidatorAddress)
				}
				if result.Signature != tc.expected.Signature {
					t.Errorf("Signature mismatch: expected %s, got %s", tc.expected.Signature, result.Signature)
				}
				if tc.expected.BlockIDFlag != core.BlockIDFlagAbsent {
					if !result.Timestamp.Equal(tc.expected.Timestamp) {
						t.Errorf("Timestamp mismatch: expected %v, got %v", tc.expected.Timestamp, result.Timestamp)
					}
				}
			}
		})
	}
}

func TestRoundTripParsing(t *testing.T) {
	testCases := []struct {
		name       string
		blockSetup func() *core.Block
	}{
		{
			name: "Simple block",
			blockSetup: func() *core.Block {
				return &core.Block{
					Header: core.Header{
						Version: core.Consensus{Block: 11, App: 0},
						ChainID: "test-chain",
						Height:  100,
						Time:    time.Now().UTC().Round(time.Second),
						LastBlockID: core.BlockID{
							Hash: randHex(32),
							PartSetHeader: core.PartSetHeader{
								Total: 10,
								Hash:  randHex(32),
							},
						},
						LastCommitHash:     randHex(32),
						DataHash:           randHex(32),
						ValidatorsHash:     randHex(32),
						NextValidatorsHash: randHex(32),
						ConsensusHash:      randHex(32),
						AppHash:            randHex(32),
						LastResultsHash:    randHex(32),
						EvidenceHash:       randHex(32),
						ProposerAddress:    randHex(20),
					},
					Data: core.Data{
						Txs: []core.Tx{},
					},
					Evidence: core.EvidenceData{
						Evidence: []core.Evidence{},
					},
					LastCommit: &core.Commit{
						Height: 99,
						Round:  0,
						BlockID: core.BlockID{
							Hash: randHex(32),
							PartSetHeader: core.PartSetHeader{
								Total: 10,
								Hash:  randHex(32),
							},
						},
						Signatures: []core.CommitSig{},
					},
				}
			},
		},
		{
			name: "Block with transactions",
			blockSetup: func() *core.Block {
				block := makeTestBlock()
				block.Data.Txs = []core.Tx{
					[]byte("tx1"),
					[]byte("tx2"),
					[]byte("tx3"),
				}
				return block
			},
		},
		{
			name: "Block with commit signatures",
			blockSetup: func() *core.Block {
				block := makeTestBlock()
				block.LastCommit.Signatures = []core.CommitSig{
					{
						BlockIDFlag:      core.BlockIDFlagCommit,
						ValidatorAddress: randHex(20),
						Timestamp:        time.Now().UTC().Round(time.Second),
						Signature:        randHex(64),
					},
					core.NewCommitSigAbsent(),
				}
				return block
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			originalBlock := tc.blockSetup()

			blockStr := originalBlock.String()

			parsedBlock, err := ParseBlockString(blockStr)
			if err != nil {
				t.Fatalf("ParseBlockString failed: %v", err)
			}
			if parsedBlock == nil {
				t.Fatal("Expected non-nil block")
			}

			if originalBlock.Header.Height != parsedBlock.Header.Height {
				t.Errorf("Height mismatch: expected %d, got %d", originalBlock.Header.Height, parsedBlock.Header.Height)
			}
			if originalBlock.Header.ChainID != parsedBlock.Header.ChainID {
				t.Errorf("ChainID mismatch: expected %s, got %s", originalBlock.Header.ChainID, parsedBlock.Header.ChainID)
			}
		})
	}
}

func makeTestBlock() *core.Block {
	return &core.Block{
		Header: core.Header{
			Version: core.Consensus{Block: 11, App: 0},
			ChainID: "test-chain-id",
			Height:  123,
			Time:    time.Now().UTC().Round(time.Second),
			LastBlockID: core.BlockID{
				Hash: randHex(32),
				PartSetHeader: core.PartSetHeader{
					Total: 100,
					Hash:  randHex(32),
				},
			},
			LastCommitHash:     randHex(32),
			DataHash:           randHex(32),
			ValidatorsHash:     randHex(32),
			NextValidatorsHash: randHex(32),
			ConsensusHash:      randHex(32),
			AppHash:            randHex(32),
			LastResultsHash:    randHex(32),
			EvidenceHash:       randHex(32),
			ProposerAddress:    randHex(20),
		},
		Data: core.Data{
			Txs: []core.Tx{},
		},
		Evidence: core.EvidenceData{
			Evidence: []core.Evidence{},
		},
		LastCommit: &core.Commit{
			Height: 122,
			Round:  1,
			BlockID: core.BlockID{
				Hash: randHex(32),
				PartSetHeader: core.PartSetHeader{
					Total: 50,
					Hash:  randHex(32),
				},
			},
			Signatures: []core.CommitSig{},
		},
	}
}
