package processor

import (
	"context"
	"fmt"
	"time"

	"github.com/bft-labs/cometbft-analyzer-types/pkg/events"
)

// ValidatorStats tracks participation metrics for a specific validator.
type ValidatorStats struct {
	ValidatorAddress string `json:"validator_address"`
	NodeID           string `json:"node_id"`
	Height           uint64 `json:"height"`
	Round            uint64 `json:"round"`

	// Vote counts
	PrevoteCount   int `json:"prevote_count"`
	PrecommitCount int `json:"precommit_count"`

	// Timing metrics
	PrevoteLatency       []int64 `json:"prevote_latency_ms"`   // milliseconds from step start
	PrecommitLatency     []int64 `json:"precommit_latency_ms"` // milliseconds from step start
	AveragePrevoteTime   int64   `json:"avg_prevote_time_ms"`
	AveragePrecommitTime int64   `json:"avg_precommit_time_ms"`

	// Participation flags
	ParticipatedPrevote   bool `json:"participated_prevote"`
	ParticipatedPrecommit bool `json:"participated_precommit"`
	OnTimePrevote         bool `json:"on_time_prevote"`   // within timeout
	OnTimePrecommit       bool `json:"on_time_precommit"` // within timeout

	// Step timing context
	PrevoteStepStart   *time.Time `json:"prevote_step_start,omitempty"`
	PrecommitStepStart *time.Time `json:"precommit_step_start,omitempty"`
}

// ValidatorParticipationProcessor tracks validator voting behavior and timing.
type ValidatorParticipationProcessor struct {
	ctx context.Context

	// Active rounds being tracked (key: "height:round:validator")
	activeValidators map[string]*ValidatorStats

	// Step start times (key: "height:round")
	stepStartTimes map[string]map[string]time.Time

	// Completed validator stats
	completedStats []interface{}
}

// NewValidatorParticipationProcessor creates a new validator participation processor.
func NewValidatorParticipationProcessor(ctx context.Context) *ValidatorParticipationProcessor {
	return &ValidatorParticipationProcessor{
		ctx:              ctx,
		activeValidators: make(map[string]*ValidatorStats),
		stepStartTimes:   make(map[string]map[string]time.Time),
		completedStats:   make([]interface{}, 0),
	}
}

// Process implements EventProcessor interface.
func (p *ValidatorParticipationProcessor) Process(evt events.Event) error {
	switch e := evt.(type) {
	case *events.EventEnteringNewRound:
		p.handleNewRound(e)
	case *events.EventEnteringPrevoteStep:
		p.handlePrevoteStepStart(e)
	case *events.EventEnteringPrecommitStep:
		p.handlePrecommitStepStart(e)
	case *events.EventSendVote:
		p.handleVoteSent(e)
	case *events.EventReceivePacketVote:
		p.handleVoteReceived(e)
	case *events.EventEnteringCommitStep:
		p.handleCommitStep(e)
	}
	return nil
}

func (p *ValidatorParticipationProcessor) handleNewRound(evt *events.EventEnteringNewRound) {
	roundKey := p.getRoundKey(evt.Height, evt.Round)

	// Initialize step timing for this round
	if p.stepStartTimes[roundKey] == nil {
		p.stepStartTimes[roundKey] = make(map[string]time.Time)
	}
	p.stepStartTimes[roundKey]["new_round"] = evt.GetTimestamp()

	// Complete previous round stats if any
	p.finalizeRoundStats(evt.Height, evt.Round-1)
}

func (p *ValidatorParticipationProcessor) handlePrevoteStepStart(evt *events.EventEnteringPrevoteStep) {
	roundKey := p.getRoundKey(evt.CurrHeight, evt.CurrRound)

	if p.stepStartTimes[roundKey] == nil {
		p.stepStartTimes[roundKey] = make(map[string]time.Time)
	}
	p.stepStartTimes[roundKey]["prevote"] = evt.GetTimestamp()

	// Initialize validator stats for this round if not exists
	p.initValidatorStats(evt.CurrHeight, evt.CurrRound, evt.GetValidatorAddress(), evt.GetNodeId())
}

func (p *ValidatorParticipationProcessor) handlePrecommitStepStart(evt *events.EventEnteringPrecommitStep) {
	roundKey := p.getRoundKey(evt.CurrHeight, evt.CurrRound)

	if p.stepStartTimes[roundKey] == nil {
		p.stepStartTimes[roundKey] = make(map[string]time.Time)
	}
	p.stepStartTimes[roundKey]["precommit"] = evt.GetTimestamp()

	// Initialize validator stats for this round if not exists
	p.initValidatorStats(evt.CurrHeight, evt.CurrRound, evt.GetValidatorAddress(), evt.GetNodeId())
}

func (p *ValidatorParticipationProcessor) handleVoteSent(evt *events.EventSendVote) {
	validatorKey := p.getValidatorKey(evt.Vote.Height, evt.Vote.Round, evt.Vote.ValidatorAddress)
	stats, exists := p.activeValidators[validatorKey]
	if !exists {
		stats = p.initValidatorStats(evt.Vote.Height, evt.Vote.Round, evt.GetValidatorAddress(), evt.GetNodeId())
	}

	roundKey := p.getRoundKey(evt.Vote.Height, evt.Vote.Round)
	stepTimes := p.stepStartTimes[roundKey]

	switch evt.Vote.Type {
	case "prevote":
		stats.PrevoteCount++
		stats.ParticipatedPrevote = true

		if stepStart, exists := stepTimes["prevote"]; exists {
			latency := evt.GetTimestamp().Sub(stepStart).Milliseconds()
			stats.PrevoteLatency = append(stats.PrevoteLatency, latency)

			// Consider on-time if within 1 second of step start (configurable threshold)
			stats.OnTimePrevote = latency <= 1000
		}
	case "precommit":
		stats.PrecommitCount++
		stats.ParticipatedPrecommit = true

		if stepStart, exists := stepTimes["precommit"]; exists {
			latency := evt.GetTimestamp().Sub(stepStart).Milliseconds()
			stats.PrecommitLatency = append(stats.PrecommitLatency, latency)

			// Consider on-time if within 1 second of step start
			stats.OnTimePrecommit = latency <= 1000
		}
	}
}

func (p *ValidatorParticipationProcessor) handleVoteReceived(evt *events.EventReceivePacketVote) {
	// For received votes, we track participation of other validators
	// This could be extended to track network-wide participation patterns

	// For now, we mainly track our own validator's voting behavior via sent votes
	// But this could be used to analyze overall network participation
}

func (p *ValidatorParticipationProcessor) handleCommitStep(evt *events.EventEnteringCommitStep) {
	// Finalize stats for this round
	p.finalizeRoundStats(evt.CurrHeight, evt.CurrRound)
}

func (p *ValidatorParticipationProcessor) initValidatorStats(height uint64, round uint64, validatorAddr, nodeID string) *ValidatorStats {
	validatorKey := p.getValidatorKey(height, round, validatorAddr)

	stats := &ValidatorStats{
		ValidatorAddress: validatorAddr,
		NodeID:           nodeID,
		Height:           height,
		Round:            round,
		PrevoteLatency:   make([]int64, 0),
		PrecommitLatency: make([]int64, 0),
	}

	p.activeValidators[validatorKey] = stats
	return stats
}

func (p *ValidatorParticipationProcessor) finalizeRoundStats(height uint64, round uint64) {
	// Find all validators for this round
	for key, stats := range p.activeValidators {
		if p.matchesRound(key, height, round) {
			// Calculate averages
			if len(stats.PrevoteLatency) > 0 {
				sum := int64(0)
				for _, latency := range stats.PrevoteLatency {
					sum += latency
				}
				stats.AveragePrevoteTime = sum / int64(len(stats.PrevoteLatency))
			}

			if len(stats.PrecommitLatency) > 0 {
				sum := int64(0)
				for _, latency := range stats.PrecommitLatency {
					sum += latency
				}
				stats.AveragePrecommitTime = sum / int64(len(stats.PrecommitLatency))
			}

			// Move to completed stats
			p.completedStats = append(p.completedStats, stats)
			delete(p.activeValidators, key)
		}
	}
}

func (p *ValidatorParticipationProcessor) getRoundKey(height uint64, round uint64) string {
	return fmt.Sprintf("%d:%d", height, round)
}

func (p *ValidatorParticipationProcessor) getValidatorKey(height uint64, round uint64, validatorAddr string) string {
	return fmt.Sprintf("%d:%d:%s", height, round, validatorAddr)
}

func (p *ValidatorParticipationProcessor) matchesRound(key string, height uint64, round uint64) bool {
	roundPrefix := fmt.Sprintf("%d:%d:", height, round)
	return len(key) > len(roundPrefix) && key[:len(roundPrefix)] == roundPrefix
}

// GetResults implements ResultsCollector interface.
func (p *ValidatorParticipationProcessor) GetResults() ([]interface{}, string) {
	// Complete any remaining active validator stats
	for key, stats := range p.activeValidators {
		// Calculate final averages
		if len(stats.PrevoteLatency) > 0 {
			sum := int64(0)
			for _, latency := range stats.PrevoteLatency {
				sum += latency
			}
			stats.AveragePrevoteTime = sum / int64(len(stats.PrevoteLatency))
		}

		if len(stats.PrecommitLatency) > 0 {
			sum := int64(0)
			for _, latency := range stats.PrecommitLatency {
				sum += latency
			}
			stats.AveragePrecommitTime = sum / int64(len(stats.PrecommitLatency))
		}

		p.completedStats = append(p.completedStats, stats)
		delete(p.activeValidators, key)
	}

	return p.completedStats, "validator_participation"
}
