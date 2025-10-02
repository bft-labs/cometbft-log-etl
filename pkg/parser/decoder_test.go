package parser

import (
	"encoding/base64"
	"encoding/hex"
	"testing"
	"time"

	logtypes "github.com/bft-labs/cometbft-log-etl/types"
	v1 "github.com/cometbft/cometbft/api/cometbft/consensus/v1"
	typesv1 "github.com/cometbft/cometbft/api/cometbft/types/v1"
	"github.com/cometbft/cometbft/types"
	"github.com/stretchr/testify/require"
)

func TestDecodeMsgBytes(t *testing.T) {
	msgBytes, err := hex.DecodeString("32B8010AB5010802100422480A20C3C61C93E975E4F6321BA3ACFBBCA6CB9678959A5B7F932A8D9B21BD5E42AF8C12240801122087DFE237E181CBB6E93EB06A7B60A205BC171E2950116B9ACF19D58FBCAA031B2A0B08C9CC93C20610C094A675321447F2A97347636F3F163AA0A80EBC148AEB00CC64380342402F889050988E20DD97116E9DD84ED0CB065F65C2F9E8B05A5CCCC321BAB7C0C777D106DFD794FE63253D574210BA8CD13B2C2166A6B07FD2F9901F50EBFA110B")
	require.NoError(t, err)

	expectedValidatorAddress, _ := hex.DecodeString("47f2a97347636f3f163aa0a80ebc148aeb00cc64")
	expectedBlockIDHash, _ := hex.DecodeString("c3c61c93e975e4f6321ba3acfbbca6cb9678959a5b7f932a8d9b21bd5e42af8c")
	expectedPartSetHeaderHash, _ := hex.DecodeString("87dfe237e181cbb6e93eb06a7b60a205bc171e2950116b9acf19d58fbcaa031b")
	expectedSignature, _ := hex.DecodeString("2f889050988e20dd97116e9dd84ed0cb065f65c2f9e8b05a5cccc321bab7c0c777d106dfd794fe63253d574210ba8cd13b2c2166a6b07fd2f9901f50ebfa110b")
	layout := "2006-01-02 15:04:05.000000 -0700"
	timeStr := "2025-06-08 01:24:25.245992 +0000"
	expectedTime, _ := time.Parse(layout, timeStr)
	expected := &typesv1.Vote{
		Type:   types.PrecommitType,
		Height: 4,
		Round:  0,
		BlockID: typesv1.BlockID{
			Hash: expectedBlockIDHash,
			PartSetHeader: typesv1.PartSetHeader{
				Total: 1,
				Hash:  expectedPartSetHeaderHash,
			},
		},
		Timestamp:          expectedTime.UTC(),
		ValidatorAddress:   expectedValidatorAddress,
		ValidatorIndex:     3,
		Signature:          expectedSignature,
		Extension:          nil,
		ExtensionSignature: nil,
	}

	result, err := DecodeMsgBytes(logtypes.VoteChannel, msgBytes)
	require.NoError(t, err)
	v, _ := result.(*typesv1.Vote)
	require.Equal(t, expected, v)
}

func TestDecodeNewRounStep(t *testing.T) {
	str, err := base64.StdEncoding.DecodeString("Cg8IARgBKP///////////wE=")
	require.NoError(t, err)

	var msg v1.Message
	err = msg.Unmarshal(str)
	require.NoError(t, err)
	newRoundStep, ok := msg.Sum.(*v1.Message_NewRoundStep)
	require.True(t, ok, "Expected NewRoundStep message type")
	require.NotNil(t, newRoundStep, "NewRoundStep should not be nil")
	require.Equal(t, int64(1), newRoundStep.NewRoundStep.Height, "Height should be 0")
	require.Equal(t, int32(0), newRoundStep.NewRoundStep.Round, "Round should be 0")
}
