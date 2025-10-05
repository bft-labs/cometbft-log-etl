package timeoutanalysis

import (
	"context"
	"fmt"
	"github.com/bft-labs/cometbft-analyzer-types/pkg/events"
	"time"
)

type TimeoutEvent struct {
	Height            uint64     `json:"height"`
	Round             uint64     `json:"round"`
	Step              string     `json:"step"`
	TimeoutType       string     `json:"timeout_type"`
	Duration          string     `json:"duration"`
	DurationMs        int64      `json:"duration_ms"`
	Timestamp         time.Time  `json:"timestamp"`
	NodeID            string     `json:"node_id"`
	ValidatorAddress  string     `json:"validator_address"`
	StepStartTime     *time.Time `json:"step_start_time,omitempty"`
	TimeInStep        int64      `json:"time_in_step_ms,omitempty"`
	IsRecoveryTimeout bool       `json:"is_recovery_timeout"`
}

type TimeoutAnalysis struct {
	NodeID              string           `json:"node_id"`
	ValidatorAddress    string           `json:"validator_address"`
	TotalTimeouts       int              `json:"total_timeouts"`
	TimeoutsByStep      map[string]int   `json:"timeouts_by_step"`
	TimeoutsByType      map[string]int   `json:"timeouts_by_type"`
	AvgTimeoutsPerRound float64          `json:"avg_timeouts_per_round"`
	MaxTimeoutsInRound  int              `json:"max_timeouts_in_round"`
	RoundsWithTimeouts  int              `json:"rounds_with_timeouts"`
	TotalRounds         int              `json:"total_rounds"`
	AvgTimeoutDuration  int64            `json:"avg_timeout_duration_ms"`
	MinTimeoutDuration  int64            `json:"min_timeout_duration_ms"`
	MaxTimeoutDuration  int64            `json:"max_timeout_duration_ms"`
	ProposeTimeouts     int              `json:"propose_timeouts"`
	PrevoteTimeouts     int              `json:"prevote_timeouts"`
	PrecommitTimeouts   int              `json:"precommit_timeouts"`
	RecoveryTimeouts    int              `json:"recovery_timeouts"`
	TimeoutClusters     []TimeoutCluster `json:"timeout_clusters,omitempty"`
	HeightRange         string           `json:"height_range"`
	FirstTimeout        *time.Time       `json:"first_timeout,omitempty"`
	LastTimeout         *time.Time       `json:"last_timeout,omitempty"`
}

type TimeoutCluster struct {
	StartHeight  uint64    `json:"start_height"`
	EndHeight    uint64    `json:"end_height"`
	TimeoutCount int       `json:"timeout_count"`
	Duration     int64     `json:"duration_ms"`
	Steps        []string  `json:"steps"`
	StartTime    time.Time `json:"start_time"`
	EndTime      time.Time `json:"end_time"`
}

type Processor struct {
	ctx               context.Context
	timeoutEvents     []interface{}
	stepStartTimes    map[string]map[string]time.Time
	roundTimeouts     map[string]int
	totalRounds       int
	totalTimeouts     int
	timeoutsByStep    map[string]int
	timeoutsByType    map[string]int
	durationSum       int64
	minDuration       int64
	maxDuration       int64
	recoveryCount     int
	minHeight         int64
	maxHeight         int64
	firstTimeout      *time.Time
	lastTimeout       *time.Time
	nodeID            string
	validatorAddress  string
	currentCluster    *TimeoutCluster
	completedClusters []TimeoutCluster
}

func NewTimeoutAnalysisProcessor(ctx context.Context) *Processor {
	return &Processor{ctx: ctx, timeoutEvents: make([]interface{}, 0), stepStartTimes: make(map[string]map[string]time.Time), roundTimeouts: make(map[string]int), timeoutsByStep: make(map[string]int), timeoutsByType: make(map[string]int), minHeight: -1, maxHeight: -1}
}

func (p *Processor) Process(evt events.Event) error {
	switch e := evt.(type) {
	case *events.EventScheduledTimeout:
		p.handleTimeout(e)
	case *events.EventEnteringNewRound:
		p.handleNewRound(e)
	case *events.EventEnteringPrevoteStep:
		p.handleStepStart(e, "prevote")
	case *events.EventEnteringPrecommitStep:
		p.handleStepStart(e, "precommit")
	case *events.EventProposeStep:
		p.handleStepStart(e, "propose")
	}
	return nil
}

func (p *Processor) handleTimeout(evt *events.EventScheduledTimeout) {
	te := &TimeoutEvent{Height: evt.Height, Round: evt.Round, Step: evt.Step, TimeoutType: evt.Step, Duration: evt.Duration, DurationMs: p.parseDurationMs(evt.Duration), Timestamp: evt.GetTimestamp(), NodeID: evt.GetNodeId(), ValidatorAddress: evt.GetValidatorAddress()}
	roundKey := p.getRoundKey(evt.Height, evt.Round)
	if stepTimes, ok := p.stepStartTimes[roundKey]; ok {
		if stepStart, ok2 := stepTimes[evt.Step]; ok2 {
			te.StepStartTime = &stepStart
			te.TimeInStep = evt.GetTimestamp().Sub(stepStart).Milliseconds()
		}
	}
	te.IsRecoveryTimeout = p.isRecoveryTimeout(evt)
	if te.IsRecoveryTimeout {
		p.recoveryCount++
	}
	p.updateTimeoutStats(te)
	p.updateTimeoutClusters(te)
	p.timeoutEvents = append(p.timeoutEvents, te)
	if p.nodeID == "" {
		p.nodeID = evt.GetNodeId()
		p.validatorAddress = evt.GetValidatorAddress()
	}
}

func (p *Processor) handleNewRound(evt *events.EventEnteringNewRound) {
	rk := p.getRoundKey(evt.Height, evt.Round)
	if p.stepStartTimes[rk] == nil {
		p.stepStartTimes[rk] = make(map[string]time.Time)
	}
	p.stepStartTimes[rk]["new_round"] = evt.GetTimestamp()
	p.totalRounds++
}

func (p *Processor) handleStepStart(evt events.Event, stepName string) {
	var height, round uint64
	var ts time.Time
	switch e := evt.(type) {
	case *events.EventEnteringPrevoteStep:
		height, round, ts = e.GetHeight(), e.GetRound(), e.GetTimestamp()
	case *events.EventEnteringPrecommitStep:
		height, round, ts = e.GetHeight(), e.GetRound(), e.GetTimestamp()
	case *events.EventProposeStep:
		height, round, ts = e.Height, e.Round, e.GetTimestamp()
	default:
		return
	}
	rk := p.getRoundKey(height, round)
	if p.stepStartTimes[rk] == nil {
		p.stepStartTimes[rk] = make(map[string]time.Time)
	}
	p.stepStartTimes[rk][stepName] = ts
}

func (p *Processor) updateTimeoutStats(te *TimeoutEvent) {
	p.totalTimeouts++
	p.timeoutsByStep[te.Step]++
	p.timeoutsByType[te.TimeoutType]++
	p.durationSum += te.DurationMs
	if p.minDuration == -1 || te.DurationMs < p.minDuration {
		p.minDuration = te.DurationMs
	}
	if te.DurationMs > p.maxDuration {
		p.maxDuration = te.DurationMs
	}
	h := int64(te.Height)
	if p.minHeight == -1 || h < p.minHeight {
		p.minHeight = h
	}
	if p.maxHeight == -1 || h > p.maxHeight {
		p.maxHeight = h
	}
	if p.firstTimeout == nil || te.Timestamp.Before(*p.firstTimeout) {
		p.firstTimeout = &te.Timestamp
	}
	if p.lastTimeout == nil || te.Timestamp.After(*p.lastTimeout) {
		p.lastTimeout = &te.Timestamp
	}
	rk := p.getRoundKey(te.Height, te.Round)
	p.roundTimeouts[rk]++
}

func (p *Processor) updateTimeoutClusters(te *TimeoutEvent) {
	if p.currentCluster == nil {
		p.currentCluster = &TimeoutCluster{StartHeight: te.Height, EndHeight: te.Height, TimeoutCount: 1, Steps: []string{te.Step}, StartTime: te.Timestamp, EndTime: te.Timestamp}
		return
	}
	since := te.Timestamp.Sub(p.currentCluster.EndTime)
	hd := te.Height - p.currentCluster.EndHeight
	if since <= 30*time.Second && hd <= 5 {
		p.currentCluster.EndHeight = te.Height
		p.currentCluster.TimeoutCount++
		p.currentCluster.Steps = append(p.currentCluster.Steps, te.Step)
		p.currentCluster.EndTime = te.Timestamp
		p.currentCluster.Duration = te.Timestamp.Sub(p.currentCluster.StartTime).Milliseconds()
	} else {
		if p.currentCluster.TimeoutCount >= 3 {
			p.completedClusters = append(p.completedClusters, *p.currentCluster)
		}
		p.currentCluster = &TimeoutCluster{StartHeight: te.Height, EndHeight: te.Height, TimeoutCount: 1, Steps: []string{te.Step}, StartTime: te.Timestamp, EndTime: te.Timestamp}
	}
}

func (p *Processor) isRecoveryTimeout(evt *events.EventScheduledTimeout) bool {
	recent := 0
	for i := uint64(0); i < 3; i++ {
		rk := p.getRoundKey(evt.Height, evt.Round-i)
		recent += p.roundTimeouts[rk]
	}
	return recent >= 2
}
func (p *Processor) parseDurationMs(d string) int64 {
	if dur, err := time.ParseDuration(d); err == nil {
		return dur.Milliseconds()
	}
	return 0
}
func (p *Processor) getRoundKey(h, r uint64) string { return fmt.Sprintf("%d:%d", h, r) }

func (p *Processor) createAnalysis() *TimeoutAnalysis {
	if p.totalTimeouts == 0 {
		return nil
	}
	a := &TimeoutAnalysis{NodeID: p.nodeID, ValidatorAddress: p.validatorAddress, TotalTimeouts: p.totalTimeouts, TimeoutsByStep: p.timeoutsByStep, TimeoutsByType: p.timeoutsByType, TotalRounds: p.totalRounds, RoundsWithTimeouts: len(p.roundTimeouts), RecoveryTimeouts: p.recoveryCount, TimeoutClusters: p.completedClusters, HeightRange: fmt.Sprintf("%d-%d", p.minHeight, p.maxHeight), FirstTimeout: p.firstTimeout, LastTimeout: p.lastTimeout}
	if p.totalRounds > 0 {
		a.AvgTimeoutsPerRound = float64(p.totalTimeouts) / float64(p.totalRounds)
	}
	if p.totalTimeouts > 0 {
		a.AvgTimeoutDuration = p.durationSum / int64(p.totalTimeouts)
		a.MinTimeoutDuration = p.minDuration
		a.MaxTimeoutDuration = p.maxDuration
	}
	for _, c := range p.roundTimeouts {
		if c > a.MaxTimeoutsInRound {
			a.MaxTimeoutsInRound = c
		}
	}
	a.ProposeTimeouts = p.timeoutsByStep["propose"]
	a.PrevoteTimeouts = p.timeoutsByStep["prevote"]
	a.PrecommitTimeouts = p.timeoutsByStep["precommit"]
	return a
}

func (p *Processor) GetResults() ([]interface{}, string) {
	results := make([]interface{}, len(p.timeoutEvents))
	copy(results, p.timeoutEvents)
	if p.currentCluster != nil && p.currentCluster.TimeoutCount >= 3 {
		p.completedClusters = append(p.completedClusters, *p.currentCluster)
	}
	if a := p.createAnalysis(); a != nil {
		results = append(results, a)
	}
	return results, "timeout_analysis"
}
