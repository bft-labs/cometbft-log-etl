package consensustiming

import (
	"context"
	"fmt"
	"time"

	"github.com/bft-labs/cometbft-analyzer-types/pkg/events"
)

type StepTiming struct {
	Height          uint64               `json:"height"`
	Round           uint64               `json:"round"`
	StepTransitions map[string]time.Time `json:"step_transitions"`
	StepDurations   map[string]int64     `json:"step_durations_ms"`
	TotalRoundTime  int64                `json:"total_round_time_ms"`
	StartTime       time.Time            `json:"start_time"`
	EndTime         *time.Time           `json:"end_time,omitempty"`
	NodeID          string               `json:"node_id"`
	ValidatorAddr   string               `json:"validator_address"`
}

type Processor struct {
	ctx              context.Context
	activeRounds     map[string]*StepTiming
	completedTimings []interface{}
}

func NewConsensusTimingProcessor(ctx context.Context) *Processor {
	return &Processor{ctx: ctx, activeRounds: make(map[string]*StepTiming), completedTimings: make([]interface{}, 0)}
}

func (p *Processor) Process(evt events.Event) error {
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

func (p *Processor) handleNewRound(evt *events.EventEnteringNewRound) {
	key := p.getRoundKey(evt.GetNodeId(), evt.Height, evt.Round)
	if existing, exists := p.activeRounds[key]; exists {
		p.completeRound(existing)
	}
	timing := &StepTiming{Height: evt.Height, Round: evt.Round, StepTransitions: make(map[string]time.Time), StepDurations: make(map[string]int64), StartTime: evt.GetTimestamp(), NodeID: evt.GetNodeId(), ValidatorAddr: evt.GetValidatorAddress()}
	timing.StepTransitions["new_round"] = evt.GetTimestamp()
	p.activeRounds[key] = timing
}

func (p *Processor) handleStepTransition(evt events.ConsensusEvent, stepName string) {
	var height, round uint64
	nodeID := evt.GetNodeId()
	validatorAddr := evt.GetValidatorAddress()
	switch e := evt.(type) {
	case *events.EventEnteringPrevoteStep, *events.EventEnteringPrevoteWaitStep, *events.EventEnteringPrecommitStep, *events.EventEnteringPrecommitWaitStep, *events.EventEnteringCommitStep, *events.EventProposeStep:
		height, round = e.GetHeight(), e.GetRound()
	default:
		return
	}
	key := p.getRoundKey(nodeID, height, round)
	timing, exists := p.activeRounds[key]
	if !exists {
		timing = &StepTiming{Height: height, Round: round, StepTransitions: make(map[string]time.Time), StepDurations: make(map[string]int64), StartTime: evt.GetTimestamp(), NodeID: nodeID, ValidatorAddr: validatorAddr}
		p.activeRounds[key] = timing
	}
	currentTime := evt.GetTimestamp()
	timing.StepTransitions[stepName] = currentTime
	p.calculateStepDuration(timing, stepName, currentTime)
}

func (p *Processor) handleCommittedBlock(evt *events.EventCommittedBlock) {
	nodeID := evt.GetNodeId()
	height := evt.GetHeight()
	currentTime := evt.GetTimestamp()
	var foundKey string
	var foundTiming *StepTiming
	for key, timing := range p.activeRounds {
		if timing.NodeID == nodeID && timing.Height == height {
			foundKey = key
			foundTiming = timing
			break
		}
	}
	if foundTiming == nil {
		return
	}
	foundTiming.StepTransitions["committed_block"] = currentTime
	p.calculateStepDuration(foundTiming, "committed_block", currentTime)
	p.completeRound(foundTiming)
	delete(p.activeRounds, foundKey)
}

func (p *Processor) calculateStepDuration(timing *StepTiming, currentStep string, currentTime time.Time) {
	stepOrder := []string{"new_round", "propose", "entering_prevote", "entering_prevote_wait", "entering_precommit", "entering_precommit_wait", "entering_commit", "committed_block"}
	var prevStepTime time.Time
	var prevStepName string
	found := false
	for i := len(stepOrder) - 1; i >= 0; i-- {
		if stepOrder[i] == currentStep {
			found = true
			continue
		}
		if found {
			if prevTime, ok := timing.StepTransitions[stepOrder[i]]; ok {
				prevStepTime = prevTime
				prevStepName = stepOrder[i]
				break
			}
		}
	}
	if !prevStepTime.IsZero() && prevStepName != "" {
		duration := currentTime.Sub(prevStepTime)
		timing.StepDurations[fmt.Sprintf("%s_to_%s", prevStepName, currentStep)] = duration.Milliseconds()
	}
}

func (p *Processor) completeRound(timing *StepTiming) {
	var endTime time.Time
	if commitTime, exists := timing.StepTransitions["committed_block"]; exists {
		endTime = commitTime
	} else {
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

func (p *Processor) getRoundKey(nodeID string, height, round uint64) string {
	return fmt.Sprintf("%s:%d:%d", nodeID, height, round)
}

func (p *Processor) GetResults() ([]interface{}, string) {
	for key, timing := range p.activeRounds {
		p.completeRound(timing)
		delete(p.activeRounds, key)
	}
	return p.completedTimings, "consensus_timing"
}
