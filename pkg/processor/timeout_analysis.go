package processor

import (
	"context"
	"fmt"
	"time"

	"github.com/bft-labs/cometbft-analyzer-types/pkg/events"
)

// TimeoutEvent represents a timeout occurrence with context.
type TimeoutEvent struct {
	Height uint64 `json:"height"`
	Round  uint64 `json:"round"`
	Step   string `json:"step"`
	// The timeoutType was added to provide a separate categorization dimension for timeout analysis. While step indicates which consensus step the
	// timeout occurred in (propose, prevote, precommit), timeoutType allows for potential future expansion where timeouts could be classified
	// differently than just by step - for example, you might want to distinguish between "normal" timeouts vs "recovery" timeouts vs "network"
	// timeouts, even if they happen in the same step.
	TimeoutType      string    `json:"timeout_type"`
	Duration         string    `json:"duration"`
	DurationMs       int64     `json:"duration_ms"`
	Timestamp        time.Time `json:"timestamp"`
	NodeID           string    `json:"node_id"`
	ValidatorAddress string    `json:"validator_address"`

	// Context information
	StepStartTime     *time.Time `json:"step_start_time,omitempty"`
	TimeInStep        int64      `json:"time_in_step_ms,omitempty"`
	IsRecoveryTimeout bool       `json:"is_recovery_timeout"`
}

// TimeoutAnalysis provides aggregated timeout statistics.
type TimeoutAnalysis struct {
	NodeID           string `json:"node_id"`
	ValidatorAddress string `json:"validator_address"`

	// Overall timeout statistics
	TotalTimeouts  int            `json:"total_timeouts"`
	TimeoutsByStep map[string]int `json:"timeouts_by_step"`
	TimeoutsByType map[string]int `json:"timeouts_by_type"`

	// Frequency analysis
	AvgTimeoutsPerRound float64 `json:"avg_timeouts_per_round"`
	MaxTimeoutsInRound  int     `json:"max_timeouts_in_round"`
	RoundsWithTimeouts  int     `json:"rounds_with_timeouts"`
	TotalRounds         int     `json:"total_rounds"`

	// Duration analysis
	AvgTimeoutDuration int64 `json:"avg_timeout_duration_ms"`
	MinTimeoutDuration int64 `json:"min_timeout_duration_ms"`
	MaxTimeoutDuration int64 `json:"max_timeout_duration_ms"`

	// Step-specific analysis
	ProposeTimeouts   int `json:"propose_timeouts"`
	PrevoteTimeouts   int `json:"prevote_timeouts"`
	PrecommitTimeouts int `json:"precommit_timeouts"`

	// Recovery patterns
	RecoveryTimeouts int              `json:"recovery_timeouts"`
	TimeoutClusters  []TimeoutCluster `json:"timeout_clusters,omitempty"`

	// Height range
	HeightRange  string     `json:"height_range"`
	FirstTimeout *time.Time `json:"first_timeout,omitempty"`
	LastTimeout  *time.Time `json:"last_timeout,omitempty"`
}

// TimeoutCluster represents a series of timeouts in rapid succession.
type TimeoutCluster struct {
	StartHeight  uint64    `json:"start_height"`
	EndHeight    uint64    `json:"end_height"`
	TimeoutCount int       `json:"timeout_count"`
	Duration     int64     `json:"duration_ms"`
	Steps        []string  `json:"steps"`
	StartTime    time.Time `json:"start_time"`
	EndTime      time.Time `json:"end_time"`
}

// TimeoutAnalysisProcessor tracks and analyzes timeout frequencies and patterns.
type TimeoutAnalysisProcessor struct {
	ctx context.Context

	// Individual timeout events
	timeoutEvents []interface{}

	// Step start times for context (key: "height:round")
	stepStartTimes map[string]map[string]time.Time

	// Round tracking for frequency analysis
	roundTimeouts map[string]int // key: "height:round"
	totalRounds   int

	// Statistics tracking
	totalTimeouts  int
	timeoutsByStep map[string]int
	timeoutsByType map[string]int
	durationSum    int64
	minDuration    int64
	maxDuration    int64
	recoveryCount  int

	// Height range
	minHeight    int64
	maxHeight    int64
	firstTimeout *time.Time
	lastTimeout  *time.Time

	// Node information
	nodeID           string
	validatorAddress string

	// Timeout clusters
	currentCluster    *TimeoutCluster
	completedClusters []TimeoutCluster
}

// NewTimeoutAnalysisProcessor creates a new timeout analysis processor.
func NewTimeoutAnalysisProcessor(ctx context.Context) *TimeoutAnalysisProcessor {
	return &TimeoutAnalysisProcessor{
		ctx:            ctx,
		timeoutEvents:  make([]interface{}, 0),
		stepStartTimes: make(map[string]map[string]time.Time),
		roundTimeouts:  make(map[string]int),
		timeoutsByStep: make(map[string]int),
		timeoutsByType: make(map[string]int),
		minHeight:      -1,
		maxHeight:      -1,
		minDuration:    -1,
		maxDuration:    -1,
	}
}

// Process implements EventProcessor interface.
func (p *TimeoutAnalysisProcessor) Process(evt events.Event) error {
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

func (p *TimeoutAnalysisProcessor) handleTimeout(evt *events.EventScheduledTimeout) {
	// Extract timeout information
	timeoutEvent := &TimeoutEvent{
		Height:           evt.Height,
		Round:            evt.Round,
		Step:             evt.Step,
		TimeoutType:      evt.Step,
		Duration:         evt.Duration,
		DurationMs:       p.parseDurationMs(evt.Duration),
		Timestamp:        evt.GetTimestamp(),
		NodeID:           evt.GetNodeId(),
		ValidatorAddress: evt.GetValidatorAddress(),
	}

	// Add step context if available
	roundKey := p.getRoundKey(evt.Height, evt.Round)
	if stepTimes, exists := p.stepStartTimes[roundKey]; exists {
		if stepStart, stepExists := stepTimes[evt.Step]; stepExists {
			timeoutEvent.StepStartTime = &stepStart
			timeoutEvent.TimeInStep = evt.GetTimestamp().Sub(stepStart).Milliseconds()
		}
	}

	// Check if this is a recovery timeout (frequent timeouts in succession)
	timeoutEvent.IsRecoveryTimeout = p.isRecoveryTimeout(evt)
	if timeoutEvent.IsRecoveryTimeout {
		p.recoveryCount++
	}

	// Update statistics
	p.updateTimeoutStats(timeoutEvent)

	// Handle timeout clusters
	p.updateTimeoutClusters(timeoutEvent)

	// Store event
	p.timeoutEvents = append(p.timeoutEvents, timeoutEvent)

	// Store node info
	if p.nodeID == "" {
		p.nodeID = evt.GetNodeId()
		p.validatorAddress = evt.GetValidatorAddress()
	}
}

func (p *TimeoutAnalysisProcessor) handleNewRound(evt *events.EventEnteringNewRound) {
	roundKey := p.getRoundKey(evt.Height, evt.Round)

	// Initialize step timing for this round
	if p.stepStartTimes[roundKey] == nil {
		p.stepStartTimes[roundKey] = make(map[string]time.Time)
	}
	p.stepStartTimes[roundKey]["new_round"] = evt.GetTimestamp()

	// Count total rounds
	p.totalRounds++
}

func (p *TimeoutAnalysisProcessor) handleStepStart(evt events.Event, stepName string) {
	var height uint64
	var round uint64
	var timestamp time.Time

	switch e := evt.(type) {
	case *events.EventEnteringPrevoteStep:
		height, round, timestamp = e.GetHeight(), e.GetRound(), e.GetTimestamp()
	case *events.EventEnteringPrecommitStep:
		height, round, timestamp = e.GetHeight(), e.GetRound(), e.GetTimestamp()
	case *events.EventProposeStep:
		height, round, timestamp = e.Height, e.Round, e.GetTimestamp()
	default:
		return
	}

	roundKey := p.getRoundKey(height, round)
	if p.stepStartTimes[roundKey] == nil {
		p.stepStartTimes[roundKey] = make(map[string]time.Time)
	}
	p.stepStartTimes[roundKey][stepName] = timestamp
}

func (p *TimeoutAnalysisProcessor) updateTimeoutStats(timeoutEvent *TimeoutEvent) {
	p.totalTimeouts++

	// Update step and type counters
	p.timeoutsByStep[timeoutEvent.Step]++
	p.timeoutsByType[timeoutEvent.TimeoutType]++

	// Update duration statistics
	p.durationSum += timeoutEvent.DurationMs
	if p.minDuration == -1 || timeoutEvent.DurationMs < p.minDuration {
		p.minDuration = timeoutEvent.DurationMs
	}
	if timeoutEvent.DurationMs > p.maxDuration {
		p.maxDuration = timeoutEvent.DurationMs
	}

	tioHeight := int64(timeoutEvent.Height)
	// Update height range
	if p.minHeight == -1 || tioHeight < p.minHeight {
		p.minHeight = tioHeight
	}
	if p.maxHeight == -1 || tioHeight > p.maxHeight {
		p.maxHeight = tioHeight
	}

	// Update time range
	if p.firstTimeout == nil || timeoutEvent.Timestamp.Before(*p.firstTimeout) {
		p.firstTimeout = &timeoutEvent.Timestamp
	}
	if p.lastTimeout == nil || timeoutEvent.Timestamp.After(*p.lastTimeout) {
		p.lastTimeout = &timeoutEvent.Timestamp
	}

	// Count timeouts per round
	roundKey := p.getRoundKey(timeoutEvent.Height, timeoutEvent.Round)
	p.roundTimeouts[roundKey]++
}

func (p *TimeoutAnalysisProcessor) updateTimeoutClusters(timeoutEvent *TimeoutEvent) {
	// If no current cluster, start a new one
	if p.currentCluster == nil {
		p.currentCluster = &TimeoutCluster{
			StartHeight:  timeoutEvent.Height,
			EndHeight:    timeoutEvent.Height,
			TimeoutCount: 1,
			Steps:        []string{timeoutEvent.Step},
			StartTime:    timeoutEvent.Timestamp,
			EndTime:      timeoutEvent.Timestamp,
		}
		return
	}

	// Check if this timeout extends the current cluster
	timeSinceLastTimeout := timeoutEvent.Timestamp.Sub(p.currentCluster.EndTime)
	heightDiff := timeoutEvent.Height - p.currentCluster.EndHeight

	// If timeout is within 30 seconds and 5 heights, extend cluster
	if timeSinceLastTimeout <= 30*time.Second && heightDiff <= 5 {
		p.currentCluster.EndHeight = timeoutEvent.Height
		p.currentCluster.TimeoutCount++
		p.currentCluster.Steps = append(p.currentCluster.Steps, timeoutEvent.Step)
		p.currentCluster.EndTime = timeoutEvent.Timestamp
		p.currentCluster.Duration = timeoutEvent.Timestamp.Sub(p.currentCluster.StartTime).Milliseconds()
	} else {
		// Complete current cluster and start new one
		if p.currentCluster.TimeoutCount >= 3 { // Only keep clusters with 3+ timeouts
			p.completedClusters = append(p.completedClusters, *p.currentCluster)
		}

		p.currentCluster = &TimeoutCluster{
			StartHeight:  timeoutEvent.Height,
			EndHeight:    timeoutEvent.Height,
			TimeoutCount: 1,
			Steps:        []string{timeoutEvent.Step},
			StartTime:    timeoutEvent.Timestamp,
			EndTime:      timeoutEvent.Timestamp,
		}
	}
}

func (p *TimeoutAnalysisProcessor) isRecoveryTimeout(evt *events.EventScheduledTimeout) bool {
	// Simple heuristic: if we've seen multiple timeouts in the last few rounds
	recentTimeouts := 0

	for i := uint64(0); i < 3; i++ { // Check last 3 rounds
		roundKey := p.getRoundKey(evt.Height, evt.Round-i)
		recentTimeouts += p.roundTimeouts[roundKey]
	}

	return recentTimeouts >= 2
}

func (p *TimeoutAnalysisProcessor) parseDurationMs(duration string) int64 {
	// Parse duration string like "1s", "500ms", etc.
	if d, err := time.ParseDuration(duration); err == nil {
		return d.Milliseconds()
	}
	return 0
}

func (p *TimeoutAnalysisProcessor) getRoundKey(height uint64, round uint64) string {
	return fmt.Sprintf("%d:%d", height, round)
}

func (p *TimeoutAnalysisProcessor) createAnalysis() *TimeoutAnalysis {
	if p.totalTimeouts == 0 {
		return nil
	}

	analysis := &TimeoutAnalysis{
		NodeID:             p.nodeID,
		ValidatorAddress:   p.validatorAddress,
		TotalTimeouts:      p.totalTimeouts,
		TimeoutsByStep:     p.timeoutsByStep,
		TimeoutsByType:     p.timeoutsByType,
		TotalRounds:        p.totalRounds,
		RoundsWithTimeouts: len(p.roundTimeouts),
		RecoveryTimeouts:   p.recoveryCount,
		TimeoutClusters:    p.completedClusters,
		HeightRange:        fmt.Sprintf("%d-%d", p.minHeight, p.maxHeight),
		FirstTimeout:       p.firstTimeout,
		LastTimeout:        p.lastTimeout,
	}

	// Calculate averages
	if p.totalRounds > 0 {
		analysis.AvgTimeoutsPerRound = float64(p.totalTimeouts) / float64(p.totalRounds)
	}

	if p.totalTimeouts > 0 {
		analysis.AvgTimeoutDuration = p.durationSum / int64(p.totalTimeouts)
		analysis.MinTimeoutDuration = p.minDuration
		analysis.MaxTimeoutDuration = p.maxDuration
	}

	// Find max timeouts in a single round
	for _, count := range p.roundTimeouts {
		if count > analysis.MaxTimeoutsInRound {
			analysis.MaxTimeoutsInRound = count
		}
	}

	// Count step-specific timeouts
	analysis.ProposeTimeouts = p.timeoutsByStep["propose"]
	analysis.PrevoteTimeouts = p.timeoutsByStep["prevote"]
	analysis.PrecommitTimeouts = p.timeoutsByStep["precommit"]

	return analysis
}

// GetResults implements ResultsCollector interface.
func (p *TimeoutAnalysisProcessor) GetResults() ([]interface{}, string) {
	results := make([]interface{}, len(p.timeoutEvents))
	copy(results, p.timeoutEvents)

	// Complete any current cluster
	if p.currentCluster != nil && p.currentCluster.TimeoutCount >= 3 {
		p.completedClusters = append(p.completedClusters, *p.currentCluster)
	}

	// Add analysis summary
	if analysis := p.createAnalysis(); analysis != nil {
		results = append(results, analysis)
	}

	return results, "timeout_analysis"
}
