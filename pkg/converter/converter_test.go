package converter

import (
	"github.com/bft-labs/cometbft-analyzer-types/pkg/core"
	"github.com/bft-labs/cometbft-analyzer-types/pkg/events"
	"github.com/bft-labs/cometbft-log-etl/lib"
	"github.com/bft-labs/cometbft-log-etl/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConvertToSpecificStepEvent(t *testing.T) {
	// Test that non-propose steps are handled correctly
	input := &types.EnteringNewStep{
		Current:    "2/0/RoundStepPropose",
		TargetStep: "prevote",
		Height:     4,
		Round:      0,
		Timestamp:  "2025-06-08T01:24:25.115023Z",
		Module:     "consensus",
		Level:      "debug",
	}

	result, err := ConvertToSpecificStepEvent(input)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Check that the result implements the Event interface
	_, ok := result.(events.Event)
	require.True(t, ok, "Result should implement events.Event interface")
}

func TestConvertToEventProposeStepOurTurn(t *testing.T) {
	input := &types.ProposeStepOurTurn{
		Height:    4,
		Round:     0,
		Proposer:  "14723CA6837129C61A873631CF7C865AEBBF63B1",
		Timestamp: "2025-06-08T01:24:25.212518Z",
		Module:    "consensus",
		Level:     "debug",
	}

	result, err := ConvertToEventProposeStepOurTurn(input)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Check that the result implements the Event interface
	var eventInterface events.Event = result
	require.NotNil(t, eventInterface, "Result should implement events.Event interface")

	// Check specific fields
	require.Equal(t, uint64(4), result.Height)
	require.Equal(t, uint64(0), result.Round)
	require.Equal(t, "14723CA6837129C61A873631CF7C865AEBBF63B1", result.Proposer)
	require.Equal(t, true, result.IsOurTurn)
}

func TestConvertToEventProposeStepNotOurTurn(t *testing.T) {
	input := &types.ProposeStepNotOurTurn{
		Height:    4,
		Round:     0,
		Proposer:  "14723CA6837129C61A873631CF7C865AEBBF63B1",
		Timestamp: "2025-06-08T01:24:25.212518Z",
		Module:    "consensus",
		Level:     "debug",
	}

	result, err := ConvertToEventProposeStepNotOurTurn(input)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Check that the result implements the Event interface
	var eventInterface events.Event = result
	require.NotNil(t, eventInterface, "Result should implement events.Event interface")

	// Check specific fields
	require.Equal(t, uint64(4), result.Height)
	require.Equal(t, uint64(0), result.Round)
	require.Equal(t, "14723CA6837129C61A873631CF7C865AEBBF63B1", result.Proposer)
	require.Equal(t, false, result.IsOurTurn)
}

func TestConvertToSpecificStepEvent_UnknownStep(t *testing.T) {
	input := &types.EnteringNewStep{
		Current:    "2/0/RoundStepPropose",
		TargetStep: "unknown_step",
		Height:     4,
		Round:      0,
		Timestamp:  "2025-06-08T01:24:25.115023Z",
		Module:     "consensus",
		Level:      "debug",
	}

	result, err := ConvertToSpecificStepEvent(input)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "unknown step type: unknown_step")
}

func TestConvert_ProposeStepHandling(t *testing.T) {
	// Test that EnteringNewStep with "propose" target step is skipped
	input := &types.EnteringNewStep{
		Current:    "4/0/RoundStepNewHeight",
		TargetStep: "propose",
		Height:     4,
		Round:      0,
		Timestamp:  "2025-06-08T01:24:25.212518Z",
		Module:     "consensus",
		Level:      "debug",
	}

	result, err := Convert(input)
	require.NoError(t, err)
	require.Nil(t, result) // Should be nil since propose steps are skipped
}

func TestConvertToEventEnteringNewRound(t *testing.T) {
	input := &types.EnteringNewRound{
		Height:    5,
		Module:    "consensus",
		Previous:  "5/0/RoundStepNewHeight",
		Proposer:  "14723CA6837129C61A873631CF7C865AEBBF63B1",
		Round:     0,
		Timestamp: "2025-06-08T01:24:26.326276Z",
		Level:     "debug",
	}
	expectedTimestamp := lib.MustParseUtcTimestamp(input.Timestamp)
	expected := &events.EventEnteringNewRound{
		BaseEvent: events.BaseEvent{
			EventType: events.EventTypeEnteringNewRound,
			Timestamp: expectedTimestamp,
		},
		Height:     5,
		Round:      0,
		Proposer:   "14723CA6837129C61A873631CF7C865AEBBF63B1",
		PrevHeight: 5,
		PrevRound:  0,
		PrevStep:   "newHeight",
	}

	result, err := ConvertToEventEnteringNewRound(input)
	if err != nil {
		require.NoError(t, err)
	}

	require.Equal(t, expected, result, "Converted event does not match expected")
}

func TestConvertToEventReceivedProposal(t *testing.T) {
	input := &types.ReceivedProposal{
		Module:    "consensus",
		Proposal:  "Proposal{1/0 (59E56586157743A042A6975F0320AAEB887A547FF01AF344321ABF8C7B2DE76B:1:711D142694C9, -1) B00C8E3F708F @ 2025-06-07T14:41:31.325889Z}",
		Proposer:  "14723CA6837129C61A873631CF7C865AEBBF63B1",
		Timestamp: "2025-06-08T01:24:20.219083Z",
		Level:     "info",
	}

	expected := &events.EventReceivedProposal{
		BaseEvent: events.BaseEvent{
			EventType: events.EventTypeReceivedProposal,
			Timestamp: lib.MustParseUtcTimestamp(input.Timestamp),
		},
		Proposal: &core.Proposal{
			Height:   1,
			Round:    0,
			PolRound: -1,
			BlockID: core.BlockID{
				Hash: "59E56586157743A042A6975F0320AAEB887A547FF01AF344321ABF8C7B2DE76B",
				PartSetHeader: core.PartSetHeader{
					Total: 1,
					Hash:  "711D142694C9",
				},
			},
			Timestamp: lib.MustParseUtcTimestamp("2025-06-07T14:41:31.325889Z"),
			Signature: "B00C8E3F708F",
		},
		Proposer: input.Proposer,
	}

	result, err := ConvertToEventReceivedProposal(input)
	require.NoError(t, err, "ConvertToEventReceivedProposal should not return an error")
	require.Equal(t, expected, result, "Converted event does not match expected")
}

func TestConvertToEventReceivedCompleteProposalBlock(t *testing.T) {
	input := &types.ReceivedCompleteProposalBlock{
		Module:    "consensus",
		Hash:      "B5B87A0CB4173DB4843C223FDD880E46117D7FD2D45B0FDCA54F5A71A3EAC56F",
		Height:    2,
		Timestamp: "2025-06-08T01:24:20.219083Z",
		Level:     "info",
	}

	expected := &events.EventReceivedCompleteProposalBlock{
		BaseEvent: events.BaseEvent{
			EventType: events.EventTypeReceivedCompleteProposalBlock,
			Timestamp: lib.MustParseUtcTimestamp(input.Timestamp),
		},
		Hash:   input.Hash,
		Height: input.Height,
	}

	result, err := ConvertToEventReceivedCompleteProposalBlock(input)
	require.NoError(t, err, "ConvertToEventReceivedCompleteProposalBlock should not return an error")
	require.Equal(t, expected, result, "Converted event does not match expected")
}

func TestConvertToEventScheduledTimeout(t *testing.T) {
	input := &types.ScheduledTimeout{
		Duration:  "3s",
		Height:    2,
		Step:      "RoundStepPropose",
		Timestamp: "2025-06-08T01:24:22.425497Z",
		Module:    "consensus",
		Level:     "debug",
	}

	expectedStep, _ := lib.FormatStep(input.Step)
	expected := &events.EventScheduledTimeout{
		BaseEvent: events.BaseEvent{
			EventType: events.EventTypeScheduledTimeout,
			Timestamp: lib.MustParseUtcTimestamp(input.Timestamp),
		},
		Duration: input.Duration,
		Height:   input.Height,
		Step:     expectedStep,
	}
	result, err := ConvertToEventScheduledTimeout(input)
	require.NoError(t, err, "ConvertToEventScheduledTimeout should not return an error")
	require.Equal(t, expected, result, "Converted event does not match expected")
}
