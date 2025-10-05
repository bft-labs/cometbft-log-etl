package networklatency

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"time"

	"github.com/bft-labs/cometbft-analyzer-types/pkg/events"
	"github.com/bft-labs/cometbft-analyzer-types/pkg/statistics/latency"
)

// ProcessorResult matches the original structure used for multi-collection returns.
type ProcessorResult struct {
	Data           []interface{}
	CollectionName string
}

// NetworkLatencyProcessor analyzes message latencies between peers to identify slow connections.
type NetworkLatencyProcessor struct {
	ctx context.Context

	// Pending sent messages waiting for receive confirmation (support multiples)
	pendingSends map[string][]*PendingSend

	// Pending received messages waiting for send events (support multiples)
	pendingReceives map[string][]*PendingReceive

	// Fallback maps keyed only by rawHash (used when peerID is unavailable, e.g., TrySend logs)
	pendingSendsByRaw    map[string][]*PendingSend
	pendingReceivesByRaw map[string][]*PendingReceive

	// Node pair latency statistics (key: normalized "nodeA:nodeB:messageType")
	nodePairStats map[string]*latency.LatencyHistogram

	// Aggregated node pair statistics (key: normalized "nodeA:nodeB")
	nodePairAggregates map[string]*latency.NodePairLatencyStats

	// Unmatched message counters
	unmatchedSendsCount    int
	unmatchedReceivesCount int

	// Message statistics
	totalSends    int
	totalReceives int

	// Per-node statistics tracking
	nodeStats map[string]*latency.NodeNetworkStats

	// Completed latency histograms
	completedHistograms []interface{}

	// Per-key traffic stats to surface duplicates and match ratios
	keyStats map[string]*KeyTrafficStats

	// Raw-hash keyed stats for cases without peer direction (e.g., TrySend)
	rawKeyStats map[string]*KeyTrafficStats
}

// NewNetworkLatencyProcessor creates a new network latency processor.
func NewNetworkLatencyProcessor(ctx context.Context) *NetworkLatencyProcessor {
	return &NetworkLatencyProcessor{
		ctx:                  ctx,
		pendingSends:         make(map[string][]*PendingSend),
		pendingReceives:      make(map[string][]*PendingReceive),
		pendingSendsByRaw:    make(map[string][]*PendingSend),
		pendingReceivesByRaw: make(map[string][]*PendingReceive),
		nodePairStats:        make(map[string]*latency.LatencyHistogram),
		nodePairAggregates:   make(map[string]*latency.NodePairLatencyStats),
		nodeStats:            make(map[string]*latency.NodeNetworkStats),
		completedHistograms:  make([]interface{}, 0),
		keyStats:             make(map[string]*KeyTrafficStats),
		rawKeyStats:          make(map[string]*KeyTrafficStats),
	}
}

// Process implements EventProcessor interface.
func (p *NetworkLatencyProcessor) Process(evt events.Event) error {
	switch e := evt.(type) {
	case *events.EventSendVote:
		p.handleSend(e, "vote")
	case *events.EventReceivePacketVote:
		p.handleReceive(e, "vote")
	case *events.EventSendProposal:
		p.handleSend(e, "proposal")
	case *events.EventReceivePacketProposal:
		p.handleReceive(e, "proposal")
	case *events.EventSendBlockPart:
		p.handleSend(e, "block_part")
	case *events.EventReceivePacketBlockPart:
		p.handleReceive(e, "block_part")
	case *events.EventSendNewRoundStep:
		p.handleSend(e, "new_round_step")
	case *events.EventReceivePacketNewRoundStep:
		p.handleReceive(e, "new_round_step")
	// Additional P2P types (optional)
	case *events.EventSendHasVote:
		p.handleSend(e, "has_vote")
	case *events.EventReceivePacketHasVote:
		p.handleReceive(e, "has_vote")
	case *events.EventSendVoteSetMaj23:
		p.handleSend(e, "vote_set_maj23")
	case *events.EventReceivePacketVoteSetMaj23:
		p.handleReceive(e, "vote_set_maj23")
	case *events.EventSendVoteSetBits:
		p.handleSend(e, "vote_set_bits")
	case *events.EventReceivePacketVoteSetBits:
		p.handleReceive(e, "vote_set_bits")
	case *events.EventSendProposalPOL:
		p.handleSend(e, "proposal_pol")
	case *events.EventReceivePacketProposalPOL:
		p.handleReceive(e, "proposal_pol")
	case *events.EventSendHasProposalBlockPart:
		p.handleSend(e, "has_proposal_block_part")
	case *events.EventReceivePacketHasProposalBlockPart:
		p.handleReceive(e, "has_proposal_block_part")
	}
	return nil
}

func (p *NetworkLatencyProcessor) handleSend(evt events.Event, messageType string) {
	peerID := p.extractPeerID(evt)

	// Use a direction-aware composite key from raw bytes when peerID is known
	compositeKey := ""
	if peerID != "" {
		compositeKey = p.getCompositeKey(evt.GetNodeId(), peerID, evt)
	}
	if compositeKey == "" && p.computeRawHash(evt) == "" {
		panic("THIS SHOULD NOT HAPPEN: send event without raw bytes")
	}

	// Update statistics only for raw-eligible events
	p.totalSends++
	p.updateNodeStats(evt.GetNodeId(), evt.GetValidatorAddress(), "send", messageType, peerID)

	// Update per-key traffic stats (send)
	ks := p.keyStats[compositeKey]
	if ks == nil {
		ks = &KeyTrafficStats{Sender: evt.GetNodeId(), Receiver: peerID, MessageType: messageType, RawHash: p.computeRawHash(evt)}
		p.keyStats[compositeKey] = ks
	}
	ks.SendCount++
	if ks.FirstSeen.IsZero() || evt.GetTimestamp().Before(ks.FirstSeen) {
		ks.FirstSeen = evt.GetTimestamp()
	}
	if evt.GetTimestamp().After(ks.LastSeen) {
		ks.LastSeen = evt.GetTimestamp()
	}
	// Check if there's already a pending receive for this message (out-of-order case)
	if compositeKey != "" {
		if recvList, exists := p.pendingReceives[compositeKey]; exists {
			if len(recvList) == 0 {
				// key exists but empty list: clean up and continue as pending send
				delete(p.pendingReceives, compositeKey)
			} else if len(recvList) > 1 {
				panic(fmt.Sprintf("multiple pending receives for key %s; this should not happen", compositeKey))
			} else {
				// Exactly one pending receive: match it
				pendingReceive := recvList[0]
				delete(p.pendingReceives, compositeKey)

				// Calculate latency (send time - receive time for out-of-order events)
				latency := evt.GetTimestamp().Sub(pendingReceive.ReceiveTime)
				latencyMs := latency.Milliseconds()

				// Record successful match
				ks.MatchCount++

				// Record latency in node pair stats
				p.recordLatency(pendingReceive.NodeID, evt.GetNodeId(), messageType, latencyMs, pendingReceive.ReceiveTime, pendingReceive.ValidatorAddr, evt.GetValidatorAddress())

				return
			}
		}
	}

	// No matching receive found, store as pending send
	var rawBytes []byte
	if rbev, ok := any(evt).(interface{ GetRawBytes() []byte }); ok {
		rb := rbev.GetRawBytes()
		if len(rb) > 0 {
			rawBytes = append([]byte(nil), rb...)
		}
	}
	ps := &PendingSend{
		RawHash:       p.computeRawHash(evt),
		RawBytes:      rawBytes,
		PeerID:        peerID,
		MessageType:   messageType,
		SentTime:      evt.GetTimestamp(),
		NodeID:        evt.GetNodeId(),
		ValidatorAddr: evt.GetValidatorAddress(),
	}
	if compositeKey != "" {
		p.pendingSends[compositeKey] = append(p.pendingSends[compositeKey], ps)
	} else {
		p.pendingSendsByRaw[ps.RawHash] = append(p.pendingSendsByRaw[ps.RawHash], ps)
		// Update raw-hash stats
		rks := p.rawKeyStats[ps.RawHash]
		if rks == nil {
			rks = &KeyTrafficStats{RawHash: ps.RawHash, MessageType: messageType}
			p.rawKeyStats[ps.RawHash] = rks
		}
		rks.SendCount++
		if rks.FirstSeen.IsZero() || evt.GetTimestamp().Before(rks.FirstSeen) {
			rks.FirstSeen = evt.GetTimestamp()
		}
		if evt.GetTimestamp().After(rks.LastSeen) {
			rks.LastSeen = evt.GetTimestamp()
		}
	}
}

func (p *NetworkLatencyProcessor) handleReceive(evt events.Event, messageType string) {
	peerID := p.extractPeerID(evt)
	if peerID == "" {
		return
	}

	// Skip self-communication (node receiving from itself)
	if evt.GetNodeId() == peerID {
		return
	}

	// Look for matching send using direction-aware composite key strictly from raw bytes
	compositeKey := p.getCompositeKey(peerID, evt.GetNodeId(), evt)
	if compositeKey == "" {
		panic("THIS SHOULD NOT HAPPEN: receive event without raw bytes cannot be matched")
	}

	// Update statistics only for raw-eligible events
	p.totalReceives++
	p.updateNodeStats(evt.GetNodeId(), evt.GetValidatorAddress(), "receive", messageType, peerID)

	// Update per-key traffic stats (receive)
	ks := p.keyStats[compositeKey]
	if ks == nil {
		ks = &KeyTrafficStats{Sender: peerID, Receiver: evt.GetNodeId(), MessageType: messageType, RawHash: p.computeRawHash(evt)}
		p.keyStats[compositeKey] = ks
	}
	ks.ReceiveCount++
	if ks.FirstSeen.IsZero() || evt.GetTimestamp().Before(ks.FirstSeen) {
		ks.FirstSeen = evt.GetTimestamp()
	}
	if evt.GetTimestamp().After(ks.LastSeen) {
		ks.LastSeen = evt.GetTimestamp()
	}
	sendList, exists := p.pendingSends[compositeKey]
	if !exists || len(sendList) == 0 {
		// No matching send found yet - store as pending receive (handles out-of-order logs)
		var rawBytes []byte
		if rbev, ok := any(evt).(interface{ GetRawBytes() []byte }); ok {
			rb := rbev.GetRawBytes()
			if len(rb) > 0 {
				rawBytes = append([]byte(nil), rb...)
			}
		}
		pr := &PendingReceive{
			RawHash:       p.computeRawHash(evt),
			RawBytes:      rawBytes,
			PeerID:        peerID,
			MessageType:   messageType,
			ReceiveTime:   evt.GetTimestamp(),
			NodeID:        evt.GetNodeId(),
			ValidatorAddr: evt.GetValidatorAddress(),
		}
		p.pendingReceives[compositeKey] = append(p.pendingReceives[compositeKey], pr)
		// Try raw-hash fallback immediate match (e.g., TrySend without peerID)
		if p.rawHashFallbackMatch(evt, messageType) {
			return
		}
		return
	}

	// Calculate latency
	// FIFO: match oldest send
	pendingSend := sendList[0]
	if len(sendList) == 1 {
		delete(p.pendingSends, compositeKey)
	} else {
		p.pendingSends[compositeKey] = sendList[1:]
	}
	elapsed := evt.GetTimestamp().Sub(pendingSend.SentTime)
	latencyMs := elapsed.Milliseconds()

	ks.MatchCount++

	// Record latency in node pair stats
	p.recordLatency(evt.GetNodeId(), pendingSend.NodeID, messageType, latencyMs, evt.GetTimestamp(), evt.GetValidatorAddress(), pendingSend.ValidatorAddr)
}

// rawHashFallbackMatch attempts to match by raw hash only when peerID is unavailable or differs.
func (p *NetworkLatencyProcessor) rawHashFallbackMatch(evt events.Event, messageType string) bool {
	rawHash := p.computeRawHash(evt)
	sendList := p.pendingSendsByRaw[rawHash]
	if len(sendList) == 0 {
		return false
	}
	// Use the oldest pending send and keep the rest for potential future matches
	pendingSend := sendList[0]
	if len(sendList) == 1 {
		delete(p.pendingSendsByRaw, rawHash)
	} else {
		p.pendingSendsByRaw[rawHash] = sendList[1:]
	}

	elapsed := evt.GetTimestamp().Sub(pendingSend.SentTime)
	latencyMs := elapsed.Milliseconds()
	// Key for stats uses unknown peer; fall back to node pair (recv node, send node)
	p.recordLatency(evt.GetNodeId(), pendingSend.NodeID, messageType, latencyMs, evt.GetTimestamp(), evt.GetValidatorAddress(), pendingSend.ValidatorAddr)
	// Update raw-hash stats duplicates and receive counts
	rks := p.rawKeyStats[rawHash]
	if rks == nil {
		rks = &KeyTrafficStats{RawHash: rawHash, MessageType: messageType}
		p.rawKeyStats[rawHash] = rks
	}
	rks.ReceiveCount++
	rks.MatchCount++
	if rks.FirstSeen.IsZero() || evt.GetTimestamp().Before(rks.FirstSeen) {
		rks.FirstSeen = evt.GetTimestamp()
	}
	if evt.GetTimestamp().After(rks.LastSeen) {
		rks.LastSeen = evt.GetTimestamp()
	}
	return true
}

func (p *NetworkLatencyProcessor) recordLatency(nodeA, nodeB, messageType string, latencyMs int64, timestamp time.Time, nodeAValidator, nodeBValidator string) {
	// Skip self-communication (node talking to itself)
	if nodeA == nodeB {
		return
	}

	// Create message type key that includes channel info if provided
	messageTypeKey := messageType

	// Create normalized node pair key
	statsKey := p.getNodePairStatsKey(nodeA, nodeB, messageTypeKey)
	pairKey, node1ID, node2ID := p.normalizeNodePair(nodeA, nodeB)

	// Determine validator addresses for normalized nodes
	var node1Validator, node2Validator string
	if node1ID == nodeA {
		node1Validator, node2Validator = nodeAValidator, nodeBValidator
	} else {
		node1Validator, node2Validator = nodeBValidator, nodeAValidator
	}

	// Get or create histogram for this node pair and message type
	if p.nodePairStats[statsKey] == nil {
		p.nodePairStats[statsKey] = &latency.LatencyHistogram{
			NodePairKey:    pairKey,
			Node1ID:        node1ID,
			Node2ID:        node2ID,
			MessageType:    messageTypeKey,
			Node1Validator: node1Validator,
			Node2Validator: node2Validator,
			Latencies:      make([]int64, 0),
			FirstSeen:      timestamp,
		}
	}

	histogram := p.nodePairStats[statsKey]

	// Add latency measurement
	histogram.Latencies = append(histogram.Latencies, latencyMs)
	histogram.Count++
	histogram.LastSeen = timestamp

	// Update min/max
	if histogram.MinLatency == 0 || latencyMs < histogram.MinLatency {
		histogram.MinLatency = latencyMs
	}
	if latencyMs > histogram.MaxLatency {
		histogram.MaxLatency = latencyMs
	}

	// Note: Percentile-based categorization is done after all data is collected
	// in calculateStatistics() function, since we need all latencies to compute percentiles
}

// normalizeNodePair creates a canonical node pair key by sorting nodes alphabetically.
func (p *NetworkLatencyProcessor) normalizeNodePair(nodeA, nodeB string) (string, string, string) {
	if nodeA <= nodeB {
		return fmt.Sprintf("%s:%s", nodeA, nodeB), nodeA, nodeB
	}
	return fmt.Sprintf("%s:%s", nodeB, nodeA), nodeB, nodeA
}

// getNodePairStatsKey creates a key for node pair statistics including message type.
func (p *NetworkLatencyProcessor) getNodePairStatsKey(nodeA, nodeB, messageType string) string {
	pairKey, _, _ := p.normalizeNodePair(nodeA, nodeB)
	return fmt.Sprintf("%s:%s", pairKey, messageType)
}

// updateNodeStats updates per-node statistics for send/receive events.
func (p *NetworkLatencyProcessor) updateNodeStats(nodeID, validatorAddr, eventType, messageType, peerID string) {
	if p.nodeStats[nodeID] == nil {
		p.nodeStats[nodeID] = &latency.NodeNetworkStats{
			NodeID:           nodeID,
			ValidatorAddress: validatorAddr,
			ConnectedPeers:   make([]string, 0),
		}
	}

	nodeStats := p.nodeStats[nodeID]

	// Update message counts
	switch eventType {
	case "send":
		nodeStats.TotalSends++
	case "receive":
		nodeStats.TotalReceives++
	}

	// Track connected peers
	p.addPeerToNode(nodeStats, peerID)
}

// addPeerToNode adds a peer to the node's connected peers list if not already present.
func (p *NetworkLatencyProcessor) addPeerToNode(nodeStats *latency.NodeNetworkStats, peerID string) {
	for _, peer := range nodeStats.ConnectedPeers {
		if peer == peerID {
			return // Already exists
		}
	}
	nodeStats.ConnectedPeers = append(nodeStats.ConnectedPeers, peerID)
	nodeStats.PeerCount = len(nodeStats.ConnectedPeers)
}

// updateNodeStatsUnmatched updates node statistics for unmatched messages.
func (p *NetworkLatencyProcessor) updateNodeStatsUnmatched(nodeID, eventType string) {
	if p.nodeStats[nodeID] == nil {
		return // Node stats not initialized
	}

	nodeStats := p.nodeStats[nodeID]

	switch eventType {
	case "send":
		nodeStats.UnmatchedSends++
	case "receive":
		nodeStats.UnmatchedReceives++
	}
}

func (p *NetworkLatencyProcessor) finalizeStats() {
	// Process any remaining pending sends as unmatched
	for _, list := range p.pendingSends {
		for _, pendingSend := range list {
			p.unmatchedSendsCount++
			p.updateNodeStatsUnmatched(pendingSend.NodeID, "send")
		}
	}
	for _, list := range p.pendingSendsByRaw {
		for _, pendingSend := range list {
			p.unmatchedSendsCount++
			p.updateNodeStatsUnmatched(pendingSend.NodeID, "send")
		}
	}

	// Process any remaining pending receives as unmatched
	for _, list := range p.pendingReceives {
		for _, pendingReceive := range list {
			p.unmatchedReceivesCount++
			p.updateNodeStatsUnmatched(pendingReceive.NodeID, "receive")
		}
	}
	for _, list := range p.pendingReceivesByRaw {
		for _, pendingReceive := range list {
			p.unmatchedReceivesCount++
			p.updateNodeStatsUnmatched(pendingReceive.NodeID, "receive")
		}
	}

	// Clear pending maps
	p.pendingSends = make(map[string][]*PendingSend)
	p.pendingReceives = make(map[string][]*PendingReceive)
	p.pendingSendsByRaw = make(map[string][]*PendingSend)
	p.pendingReceivesByRaw = make(map[string][]*PendingReceive)

	// Finalize individual histograms
	for _, histogram := range p.nodePairStats {
		p.calculateStatistics(histogram)
		p.completedHistograms = append(p.completedHistograms, histogram)
	}

	// Create aggregated node pair stats
	p.createNodePairAggregates()

	// Add aggregated stats to results
	for _, aggregate := range p.nodePairAggregates {
		p.completedHistograms = append(p.completedHistograms, aggregate)
	}
}

func (p *NetworkLatencyProcessor) calculateStatistics(histogram *latency.LatencyHistogram) {
	if len(histogram.Latencies) == 0 {
		return
	}

	// Sort latencies for percentile calculations
	latencies := make([]int64, len(histogram.Latencies))
	copy(latencies, histogram.Latencies)
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	// Calculate mean
	sum := int64(0)
	for _, latency := range latencies {
		sum += latency
	}
	histogram.MeanLatency = sum / int64(len(latencies))

	// Calculate median
	mid := len(latencies) / 2
	if len(latencies)%2 == 0 {
		histogram.MedianLatency = (latencies[mid-1] + latencies[mid]) / 2
	} else {
		histogram.MedianLatency = latencies[mid]
	}

	// Calculate percentiles
	p95Index := int(float64(len(latencies)) * 0.95)
	p99Index := int(float64(len(latencies)) * 0.99)

	if p95Index >= len(latencies) {
		p95Index = len(latencies) - 1
	}
	if p99Index >= len(latencies) {
		p99Index = len(latencies) - 1
	}

	histogram.P95Latency = latencies[p95Index]
	histogram.P99Latency = latencies[p99Index]

	// Percentile-based categorization
	p50Threshold := histogram.MedianLatency
	p95Threshold := histogram.P95Latency
	p99Threshold := histogram.P99Latency

	for _, latency := range latencies {
		if latency < p50Threshold {
			histogram.BelowP50Count++
		} else if latency < p95Threshold {
			histogram.P50ToP95Count++
		} else if latency < p99Threshold {
			histogram.P95ToP99Count++
		} else {
			histogram.AboveP99Count++
		}
	}
}

// createNodePairAggregates creates aggregated statistics for each node pair across all message types.
func (p *NetworkLatencyProcessor) createNodePairAggregates() {
	// Group histograms by node pair
	pairGroups := make(map[string][]*latency.LatencyHistogram)

	for _, histogram := range p.nodePairStats {
		pairKey := histogram.NodePairKey
		pairGroups[pairKey] = append(pairGroups[pairKey], histogram)
	}

	// Create aggregate for each pair
	for pairKey, histograms := range pairGroups {
		if len(histograms) == 0 {
			continue
		}

		// Use first histogram for metadata
		first := histograms[0]
		aggregate := &latency.NodePairLatencyStats{
			NodePairKey:    pairKey,
			Node1ID:        first.Node1ID,
			Node2ID:        first.Node2ID,
			Node1Validator: first.Node1Validator,
			Node2Validator: first.Node2Validator,
			MessageTypes:   make(map[string]*latency.LatencyHistogram),
		}

		// Add all histograms for this pair
		for _, hist := range histograms {
			aggregate.MessageTypes[hist.MessageType] = hist
		}

		// Create overall stats for this pair
		aggregate.OverallStats = p.createOverallStatsForPair(histograms, pairKey, first.Node1ID, first.Node2ID, first.Node1Validator, first.Node2Validator)

		p.nodePairAggregates[pairKey] = aggregate
	}
}

func (p *NetworkLatencyProcessor) createOverallStatsForPair(histograms []*latency.LatencyHistogram, pairKey, node1ID, node2ID, node1Validator, node2Validator string) *latency.LatencyHistogram {
	allLatencies := make([]int64, 0)
	var firstSeen, lastSeen time.Time
	// Aggregate latencies from all histograms
	minLatency, maxLatency := int64(0), int64(0)

	for _, histogram := range histograms {
		allLatencies = append(allLatencies, histogram.Latencies...)

		if firstSeen.IsZero() || histogram.FirstSeen.Before(firstSeen) {
			firstSeen = histogram.FirstSeen
		}
		if histogram.LastSeen.After(lastSeen) {
			lastSeen = histogram.LastSeen
		}

		if minLatency == 0 || histogram.MinLatency < minLatency {
			minLatency = histogram.MinLatency
		}
		if histogram.MaxLatency > maxLatency {
			maxLatency = histogram.MaxLatency
		}
	}

	if len(allLatencies) == 0 {
		return nil
	}

	overallHist := &latency.LatencyHistogram{
		NodePairKey:    pairKey,
		Node1ID:        node1ID,
		Node2ID:        node2ID,
		MessageType:    "overall",
		Node1Validator: node1Validator,
		Node2Validator: node2Validator,
		Latencies:      allLatencies,
		Count:          len(allLatencies),
		MinLatency:     minLatency,
		MaxLatency:     maxLatency,
		FirstSeen:      firstSeen,
		LastSeen:       lastSeen,
	}

	// Let calculateStatistics compute all percentile-based statistics correctly
	p.calculateStatistics(overallHist)
	return overallHist
}

// Helper methods for extracting peer IDs and creating message keys
func (p *NetworkLatencyProcessor) extractPeerID(evt events.Event) string {
	// Prefer generic accessor if available
	if x, ok := any(evt).(interface{ GetRecipientPeerId() string }); ok {
		return x.GetRecipientPeerId()
	}
	if x, ok := any(evt).(interface{ GetSourcePeerId() string }); ok {
		return x.GetSourcePeerId()
	}
	// Extract peer ID from different event types
	switch e := evt.(type) {
	case *events.EventSendVote:
		return e.RecipientPeerId
	case *events.EventReceivePacketVote:
		return e.SourcePeerId
	case *events.EventSendProposal:
		return e.RecipientPeerId
	case *events.EventReceivePacketProposal:
		return e.SourcePeerId
	case *events.EventSendBlockPart:
		return e.RecipientPeerId
	case *events.EventReceivePacketBlockPart:
		return e.SourcePeerId
	case *events.EventSendNewRoundStep:
		return e.RecipientPeerId
	case *events.EventReceivePacketNewRoundStep:
		return e.SourcePeerId
	case *events.EventSendHasVote:
		return e.RecipientPeerId
	case *events.EventReceivePacketHasVote:
		return e.SourcePeerId
	case *events.EventSendVoteSetMaj23:
		return e.RecipientPeerId
	case *events.EventReceivePacketVoteSetMaj23:
		return e.SourcePeerId
	case *events.EventSendVoteSetBits:
		return e.RecipientPeerId
	case *events.EventReceivePacketVoteSetBits:
		return e.SourcePeerId
	case *events.EventSendProposalPOL:
		return e.RecipientPeerId
	case *events.EventReceivePacketProposalPOL:
		return e.SourcePeerId
	case *events.EventSendHasProposalBlockPart:
		return e.RecipientPeerId
	case *events.EventReceivePacketHasProposalBlockPart:
		return e.SourcePeerId
	}
	return ""
}

// getCompositeKey creates a direction-aware key combining sender, receiver, and raw-bytes hash (via GetRawBytes)
func (p *NetworkLatencyProcessor) getCompositeKey(sender, receiver string, evt events.Event) string {
	if rbev, ok := any(evt).(interface{ GetRawBytes() []byte }); ok {
		raw := rbev.GetRawBytes()
		if len(raw) == 0 {
			return ""
		}
		sum := sha256.Sum256(raw)
		return fmt.Sprintf("%s:%s:%x", sender, receiver, sum[:])
	}
	return ""
}

// computeRawHash returns sha256 of event raw bytes, if provided by the event
func (p *NetworkLatencyProcessor) computeRawHash(evt events.Event) string {
	if rbev, ok := any(evt).(interface{ GetRawBytes() []byte }); ok {
		sum := sha256.Sum256(rbev.GetRawBytes())
		return fmt.Sprintf("%x", sum[:])
	}
	return ""
}

// createUnmatchedStats generates statistics about unmatched messages.
func (p *NetworkLatencyProcessor) createUnmatchedStats() *latency.UnmatchedMessageStats {
	// Return nil if no unmatched messages
	if p.unmatchedSendsCount == 0 && p.unmatchedReceivesCount == 0 {
		return nil
	}

	stats := &latency.UnmatchedMessageStats{
		TotalUnmatchedSends:    p.unmatchedSendsCount,
		TotalUnmatchedReceives: p.unmatchedReceivesCount,
		TotalMessages:          p.totalSends + p.totalReceives,
	}

	return stats
}

// finalizeNodeStats calculates final statistics for each node.
func (p *NetworkLatencyProcessor) finalizeNodeStats() []interface{} {
	nodeResults := make([]interface{}, 0)

	for _, nodeStats := range p.nodeStats {
		// Add custom logic if needed to finalize node stats
		nodeResults = append(nodeResults, nodeStats)
	}

	return nodeResults
}

// GetResults implements ResultsCollector interface.
// For backward compatibility, returns empty results as this processor now uses GetMultiResults.
func (p *NetworkLatencyProcessor) GetResults() ([]interface{}, string) {
	return []interface{}{}, ""
}

// GetMultiResults implements MultiCollectionResultsCollector interface.
func (p *NetworkLatencyProcessor) GetMultiResults() []ProcessorResult {
	// Finalize regular stats
	p.finalizeStats()

	results := []ProcessorResult{}

	// 1. Individual latency measurements (LatencyHistogram)
	measurements := make([]interface{}, 0)
	for _, histogram := range p.nodePairStats {
		measurements = append(measurements, histogram)
	}
	if len(measurements) > 0 {
		results = append(results, ProcessorResult{
			Data:           measurements,
			CollectionName: "network_latency_measurements",
		})
	}

	// 2. Node pair summary statistics (NodePairLatencyStats)
	nodePairSummaries := make([]interface{}, 0)
	for _, aggregate := range p.nodePairAggregates {
		nodePairSummaries = append(nodePairSummaries, aggregate)
	}
	if len(nodePairSummaries) > 0 {
		results = append(results, ProcessorResult{
			Data:           nodePairSummaries,
			CollectionName: "network_latency_nodepair_summary",
		})
	}

	// 3. Per-node statistics (NodeNetworkStats)
	nodeStats := p.finalizeNodeStats()
	if len(nodeStats) > 0 {
		results = append(results, ProcessorResult{
			Data:           nodeStats,
			CollectionName: "network_latency_node_stats",
		})
	}

	// 4. Global unmatched message statistics (UnmatchedMessageStats)
	if unmatchedStats := p.createUnmatchedStats(); unmatchedStats != nil {
		results = append(results, ProcessorResult{
			Data:           []interface{}{unmatchedStats},
			CollectionName: "network_latency_global_stats",
		})
	}

	// 6. Duplicate traffic diagnostics per composite key
	duplicates := make([]interface{}, 0)
	for _, s := range p.keyStats {
		if s.SendCount > 1 || s.ReceiveCount > 1 {
			duplicates = append(duplicates, s)
		}
	}
	// Include raw-hash keyed duplicates as well
	for _, s := range p.rawKeyStats {
		if s.SendCount > 1 || s.ReceiveCount > 1 {
			duplicates = append(duplicates, s)
		}
	}
	if len(duplicates) > 0 {
		results = append(results, ProcessorResult{
			Data:           duplicates,
			CollectionName: "network_latency_duplicates_debug",
		})
	}

	return results
}

// PendingSend tracks a sent message waiting for receive confirmation.
type PendingSend struct {
	RawHash       string
	RawBytes      []byte
	PeerID        string
	MessageType   string
	SentTime      time.Time
	NodeID        string
	ValidatorAddr string
}

// PendingReceive tracks a received message waiting for send event (handles out-of-order logs).
type PendingReceive struct {
	RawHash       string
	RawBytes      []byte
	PeerID        string // The peer who sent this message
	MessageType   string
	ReceiveTime   time.Time
	NodeID        string // The node that received this message
	ValidatorAddr string
}

// KeyTrafficStats summarizes per-composite-key traffic to expose duplicates
type KeyTrafficStats struct {
	Sender       string    `json:"sender"`
	Receiver     string    `json:"receiver"`
	MessageType  string    `json:"messageType"`
	RawHash      string    `json:"rawHash"`
	SendCount    int       `json:"sendCount"`
	ReceiveCount int       `json:"receiveCount"`
	MatchCount   int       `json:"matchCount"`
	FirstSeen    time.Time `json:"firstSeen"`
	LastSeen     time.Time `json:"lastSeen"`
}
