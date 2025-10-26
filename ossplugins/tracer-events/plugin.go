package tracerevents

import (
	"context"
	"sort"

	analyzer "github.com/bft-labs/cometbft-analyzer-types/pkg/events"
	consensussteps "github.com/bft-labs/cometbft-log-etl/ossplugins/consensus-steps"
	p2pmessages "github.com/bft-labs/cometbft-log-etl/ossplugins/p2p-messages"
	"github.com/bft-labs/cometbft-log-etl/pkg/pluginloader"
	"github.com/bft-labs/cometbft-log-etl/pkg/pluginsdk"
)

// Plugin aggregates consensus step events with confirmed P2P events
// and stores them as a unified stream for the tracer UI.
type Plugin struct {
	ctx pluginsdk.Context
	// internal processors we reuse
	cs interface {
		Process(analyzer.Event) error
		GetResults() ([]interface{}, string)
	}
	p2p interface {
		Process(analyzer.Event) error
		GetResults() ([]interface{}, string)
	}
}

func (p *Plugin) Name() string { return "tracer-events" }

func (p *Plugin) Init(ctx pluginsdk.Context) error {
	p.ctx = ctx
	// reuse existing processors to avoid duplicating logic
	p.cs = consensussteps.NewProcessor(ctx.Ctx)
	p.p2p = p2pmessages.NewP2pMessageProcessor(ctx.Ctx)
	return nil
}

func (p *Plugin) Process(event interface{}) error {
	if evt, ok := event.(analyzer.Event); ok {
		// forward to both processors
		_ = p.cs.Process(evt)
		_ = p.p2p.Process(evt)
	}
	return nil
}

func (p *Plugin) Finalize() error {
	// collect results from both processors
	cData, _ := p.cs.GetResults()
	p2pData, _ := p.p2p.GetResults()

	combined := make([]interface{}, 0, len(cData)+len(p2pData))
	combined = append(combined, cData...)
	combined = append(combined, p2pData...)

	if len(combined) == 0 {
		return nil
	}

	// sort by timestamp if possible
	sort.Slice(combined, func(i, j int) bool {
		ei, okI := combined[i].(analyzer.Event)
		ej, okJ := combined[j].(analyzer.Event)
		if !okI || !okJ {
			return false
		}
		return ei.GetTimestamp().Before(ej.GetTimestamp())
	})

	// store into tracer_events collection
	return p.ctx.Storage.StoreResults(context.Background(), combined, "tracer_events")
}

func init() { pluginloader.Register("tracer-events", func() pluginsdk.Plugin { return &Plugin{} }) }
