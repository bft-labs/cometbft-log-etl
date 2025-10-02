package processor

import (
	"context"
	"fmt"
	"time"

	"github.com/bft-labs/cometbft-analyzer-types/pkg/events"
)

// StepTiming tracks timing data for consensus steps within a specific height/round.
type StepTiming struct {
	Height          uint64               `json:"height"`
	Round           uint64               `json:"round"`
	StepTransitions map[string]time.Time `json:"step_transitions"`
	StepDurations   map[string]int64     `json:"step_durations_ms"` // milliseconds
	TotalRoundTime  int64                `json:"total_round_time_ms"`
	StartTime       time.Time            `json:"start_time"`
	EndTime         *time.Time           `json:"end_time,omitempty"`
	NodeID          string               `json:"node_id"`
	ValidatorAddr   string               `json:"validator_address"`
}

// ConsensusTimingProcessor measures durations between consensus step transitions.
type ConsensusTimingProcessor struct {
	ctx context.Context

	// Active rounds being tracked (key: "nodeID:height:round")
	activeRounds map[string]*StepTiming

	// Completed timing data
	completedTimings []interface{}
}

// NewConsensusTimingProcessor creates a new consensus timing processor.
func NewConsensusTimingProcessor(ctx context.Context) *ConsensusTimingProcessor {
	return &ConsensusTimingProcessor{
		ctx:              ctx,
		activeRounds:     make(map[string]*StepTiming),
		completedTimings: make([]interface{}, 0),
	}
}

// Process implements EventProcessor interface.
func (p *ConsensusTimingProcessor) Process(evt events.Event) error {
	switch e := evt.(type) {
	case *events.EventEnteringNewRound:
		p.handleNewRound(e)
	case *events.EventProposeStep:
		p.handleStepTransition(e, "propose")
	case *events.EventEnteringPrevoteStep:
		p.handleStepTransition(e, "entering_prevote")
	case *events.EventEnteringPrevoteWaitStep:
		p.handleStepTransition(e, "entering_prevote_wait")
	case *events.EventEnteringPrecommitStep:
		p.handleStepTransition(e, "entering_precommit")
	case *events.EventEnteringPrecommitWaitStep:
		p.handleStepTransition(e, "entering_precommit_wait")
	case *events.EventEnteringCommitStep:
		p.handleStepTransition(e, "entering_commit")
	case *events.EventCommittedBlock:
		p.handleCommittedBlock(e)
	}
	return nil
}

func (p *ConsensusTimingProcessor) handleNewRound(evt *events.EventEnteringNewRound) {
	key := p.getRoundKey(evt.GetNodeId(), evt.Height, evt.Round)

	// Complete previous round if exists
	if existing, exists := p.activeRounds[key]; exists {
		p.completeRound(existing)
	}

	// Start new round timing
	timing := &StepTiming{
		Height:          evt.Height,
		Round:           evt.Round,
		StepTransitions: make(map[string]time.Time),
		StepDurations:   make(map[string]int64),
		StartTime:       evt.GetTimestamp(),
		NodeID:          evt.GetNodeId(),
		ValidatorAddr:   evt.GetValidatorAddress(),
	}

	timing.StepTransitions["new_round"] = evt.GetTimestamp()
	p.activeRounds[key] = timing
}

func (p *ConsensusTimingProcessor) handleStepTransition(evt events.ConsensusEvent, stepName string) {
	var height uint64
	var round uint64
	nodeID := evt.GetNodeId()
	validatorAddr := evt.GetValidatorAddress()

	// Extract height/round from different event types
	switch e := evt.(type) {
	case *events.EventEnteringPrevoteStep, *events.EventEnteringPrevoteWaitStep,
		*events.EventEnteringPrecommitStep, *events.EventEnteringPrecommitWaitStep,
		*events.EventEnteringCommitStep, *events.EventProposeStep:
		height, round = e.GetHeight(), e.GetRound()
	default:
		return
	}

	key := p.getRoundKey(nodeID, height, round)
	timing, exists := p.activeRounds[key]
	if !exists {
		// Create timing entry if we missed the new round event
		timing = &StepTiming{
			Height:          height,
			Round:           round,
			StepTransitions: make(map[string]time.Time),
			StepDurations:   make(map[string]int64),
			StartTime:       evt.GetTimestamp(),
			NodeID:          nodeID,
			ValidatorAddr:   validatorAddr,
		}
		p.activeRounds[key] = timing
	}

	currentTime := evt.GetTimestamp()
	timing.StepTransitions[stepName] = currentTime

	// Calculate duration from previous step
	p.calculateStepDuration(timing, stepName, currentTime)
}

func (p *ConsensusTimingProcessor) handleCommittedBlock(evt *events.EventCommittedBlock) {
	nodeID := evt.GetNodeId()
	height := evt.GetHeight()
	currentTime := evt.GetTimestamp()

	// Find the active round for this node and height
	// We need to search through all active rounds since we don't know the round number
	var foundKey string
	var foundTiming *StepTiming

	for key, timing := range p.activeRounds {
		// Check if this timing matches the node and height
		if timing.NodeID == nodeID && timing.Height == height {
			foundKey = key
			foundTiming = timing
			break
		}
	}

	if foundTiming == nil {
		// No active round found for this committed block
		// This can happen if we missed the earlier events
		return
	}

	// Record the committed block transition
	foundTiming.StepTransitions["committed_block"] = currentTime

	// Calculate duration from previous step
	p.calculateStepDuration(foundTiming, "committed_block", currentTime)

	// Complete the round since block is committed
	p.completeRound(foundTiming)
	delete(p.activeRounds, foundKey)
}

func (p *ConsensusTimingProcessor) calculateStepDuration(timing *StepTiming, currentStep string, currentTime time.Time) {
	stepOrder := []string{
		"new_round", "propose", "entering_prevote", "entering_prevote_wait",
		"entering_precommit", "entering_precommit_wait", "entering_commit", "committed_block"}

	var prevStepTime time.Time
	var prevStepName string
	found := false

	// Find the most recent previous step
	for i := len(stepOrder) - 1; i >= 0; i-- {
		if stepOrder[i] == currentStep {
			found = true
			continue
		}
		if found {
			if prevTime, exists := timing.StepTransitions[stepOrder[i]]; exists {
				prevStepTime = prevTime
				prevStepName = stepOrder[i]
				break
			}
		}
	}

	if !prevStepTime.IsZero() && prevStepName != "" {
		duration := currentTime.Sub(prevStepTime)
		// Use clear naming: "from_step_to_step"
		durationKey := fmt.Sprintf("%s_to_%s", prevStepName, currentStep)
		timing.StepDurations[durationKey] = duration.Milliseconds()
	}
}

func (p *ConsensusTimingProcessor) completeRound(timing *StepTiming) {
	// Calculate total round time
	var endTime time.Time
	if commitTime, exists := timing.StepTransitions["committed_block"]; exists {
		endTime = commitTime
	} else {
		// Use the latest step transition
		for _, stepTime := range timing.StepTransitions {
			if stepTime.After(endTime) {
				endTime = stepTime
			}
		}
	}

	if !endTime.IsZero() {
		timing.EndTime = &endTime
		timing.TotalRoundTime = endTime.Sub(timing.StartTime).Milliseconds()
	}

	p.completedTimings = append(p.completedTimings, timing)
}

func (p *ConsensusTimingProcessor) getRoundKey(nodeID string, height uint64, round uint64) string {
	return fmt.Sprintf("%s:%d:%d", nodeID, height, round)
}

// GetResults implements ResultsCollector interface.
func (p *ConsensusTimingProcessor) GetResults() ([]interface{}, string) {
	// Complete any remaining active rounds
	for key, timing := range p.activeRounds {
		p.completeRound(timing)
		delete(p.activeRounds, key)
	}

	return p.completedTimings, "consensus_timing"
}
