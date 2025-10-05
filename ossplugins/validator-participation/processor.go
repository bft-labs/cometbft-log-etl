package validatorparticipation

import (
	"context"
	"fmt"
	"github.com/bft-labs/cometbft-analyzer-types/pkg/events"
	"time"
)

type ValidatorStats struct {
	ValidatorAddress      string     `json:"validator_address"`
	NodeID                string     `json:"node_id"`
	Height                uint64     `json:"height"`
	Round                 uint64     `json:"round"`
	PrevoteCount          int        `json:"prevote_count"`
	PrecommitCount        int        `json:"precommit_count"`
	PrevoteLatency        []int64    `json:"prevote_latency_ms"`
	PrecommitLatency      []int64    `json:"precommit_latency_ms"`
	AveragePrevoteTime    int64      `json:"avg_prevote_time_ms"`
	AveragePrecommitTime  int64      `json:"avg_precommit_time_ms"`
	ParticipatedPrevote   bool       `json:"participated_prevote"`
	ParticipatedPrecommit bool       `json:"participated_precommit"`
	OnTimePrevote         bool       `json:"on_time_prevote"`
	OnTimePrecommit       bool       `json:"on_time_precommit"`
	PrevoteStepStart      *time.Time `json:"prevote_step_start,omitempty"`
	PrecommitStepStart    *time.Time `json:"precommit_step_start,omitempty"`
}

type Processor struct {
	ctx              context.Context
	activeValidators map[string]*ValidatorStats
	stepStartTimes   map[string]map[string]time.Time
	completedStats   []interface{}
}

func NewValidatorParticipationProcessor(ctx context.Context) *Processor {
	return &Processor{ctx: ctx, activeValidators: make(map[string]*ValidatorStats), stepStartTimes: make(map[string]map[string]time.Time), completedStats: make([]interface{}, 0)}
}

func (p *Processor) Process(evt events.Event) error {
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

func (p *Processor) handleNewRound(evt *events.EventEnteringNewRound) {
	roundKey := p.getRoundKey(evt.Height, evt.Round)
	if p.stepStartTimes[roundKey] == nil {
		p.stepStartTimes[roundKey] = make(map[string]time.Time)
	}
	p.stepStartTimes[roundKey]["new_round"] = evt.GetTimestamp()
	p.finalizeRoundStats(evt.Height, evt.Round-1)
}

func (p *Processor) handlePrevoteStepStart(evt *events.EventEnteringPrevoteStep) {
	roundKey := p.getRoundKey(evt.CurrHeight, evt.CurrRound)
	if p.stepStartTimes[roundKey] == nil {
		p.stepStartTimes[roundKey] = make(map[string]time.Time)
	}
	p.stepStartTimes[roundKey]["prevote"] = evt.GetTimestamp()
	p.initValidatorStats(evt.CurrHeight, evt.CurrRound, evt.GetValidatorAddress(), evt.GetNodeId())
}

func (p *Processor) handlePrecommitStepStart(evt *events.EventEnteringPrecommitStep) {
	roundKey := p.getRoundKey(evt.CurrHeight, evt.CurrRound)
	if p.stepStartTimes[roundKey] == nil {
		p.stepStartTimes[roundKey] = make(map[string]time.Time)
	}
	p.stepStartTimes[roundKey]["precommit"] = evt.GetTimestamp()
	p.initValidatorStats(evt.CurrHeight, evt.CurrRound, evt.GetValidatorAddress(), evt.GetNodeId())
}

func (p *Processor) handleVoteSent(evt *events.EventSendVote) {
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
		if stepStart, ok := stepTimes["prevote"]; ok {
			latency := evt.GetTimestamp().Sub(stepStart).Milliseconds()
			stats.PrevoteLatency = append(stats.PrevoteLatency, latency)
			stats.OnTimePrevote = latency <= 1000
		}
	case "precommit":
		stats.PrecommitCount++
		stats.ParticipatedPrecommit = true
		if stepStart, ok := stepTimes["precommit"]; ok {
			latency := evt.GetTimestamp().Sub(stepStart).Milliseconds()
			stats.PrecommitLatency = append(stats.PrecommitLatency, latency)
			stats.OnTimePrecommit = latency <= 1000
		}
	}
}

func (p *Processor) handleVoteReceived(_ *events.EventReceivePacketVote) {}

func (p *Processor) handleCommitStep(evt *events.EventEnteringCommitStep) {
	p.finalizeRoundStats(evt.CurrHeight, evt.CurrRound)
}

func (p *Processor) initValidatorStats(height, round uint64, validatorAddr, nodeID string) *ValidatorStats {
	validatorKey := p.getValidatorKey(height, round, validatorAddr)
	stats := &ValidatorStats{ValidatorAddress: validatorAddr, NodeID: nodeID, Height: height, Round: round, PrevoteLatency: make([]int64, 0), PrecommitLatency: make([]int64, 0)}
	p.activeValidators[validatorKey] = stats
	return stats
}

func (p *Processor) finalizeRoundStats(height, round uint64) {
	for key, stats := range p.activeValidators {
		if p.matchesRound(key, height, round) {
			if len(stats.PrevoteLatency) > 0 {
				sum := int64(0)
				for _, l := range stats.PrevoteLatency {
					sum += l
				}
				stats.AveragePrevoteTime = sum / int64(len(stats.PrevoteLatency))
			}
			if len(stats.PrecommitLatency) > 0 {
				sum := int64(0)
				for _, l := range stats.PrecommitLatency {
					sum += l
				}
				stats.AveragePrecommitTime = sum / int64(len(stats.PrecommitLatency))
			}
			p.completedStats = append(p.completedStats, stats)
			delete(p.activeValidators, key)
		}
	}
}

func (p *Processor) getRoundKey(height, round uint64) string {
	return fmt.Sprintf("%d:%d", height, round)
}
func (p *Processor) getValidatorKey(height, round uint64, validatorAddr string) string {
	return fmt.Sprintf("%d:%d:%s", height, round, validatorAddr)
}
func (p *Processor) matchesRound(key string, height, round uint64) bool {
	prefix := fmt.Sprintf("%d:%d:", height, round)
	return len(key) > len(prefix) && key[:len(prefix)] == prefix
}

func (p *Processor) GetResults() ([]interface{}, string) {
	for key, stats := range p.activeValidators {
		if len(stats.PrevoteLatency) > 0 {
			sum := int64(0)
			for _, l := range stats.PrevoteLatency {
				sum += l
			}
			stats.AveragePrevoteTime = sum / int64(len(stats.PrevoteLatency))
		}
		if len(stats.PrecommitLatency) > 0 {
			sum := int64(0)
			for _, l := range stats.PrecommitLatency {
				sum += l
			}
			stats.AveragePrecommitTime = sum / int64(len(stats.PrecommitLatency))
		}
		p.completedStats = append(p.completedStats, stats)
		delete(p.activeValidators, key)
	}
	return p.completedStats, "validator_participation"
}
