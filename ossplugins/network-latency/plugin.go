package networklatency

import (
	"context"
	analyzer "github.com/bft-labs/cometbft-analyzer-types/pkg/events"
	"github.com/bft-labs/cometbft-log-etl/pkg/pluginloader"
	"github.com/bft-labs/cometbft-log-etl/pkg/pluginsdk"
)

type Plugin struct {
	ctx pluginsdk.Context
	ep  interface {
		Process(analyzer.Event) error
		GetMultiResults() []ProcessorResult
		GetResults() ([]interface{}, string)
	}
}

func (p *Plugin) Name() string { return "network-latency" }
func (p *Plugin) Init(ctx pluginsdk.Context) error {
	p.ctx = ctx
	p.ep = NewNetworkLatencyProcessor(ctx.Ctx)
	return nil
}
func (p *Plugin) Process(event interface{}) error {
	if evt, ok := event.(analyzer.Event); ok {
		return p.ep.Process(evt)
	}
	return nil
}
func (p *Plugin) Finalize() error {
	for _, res := range p.ep.GetMultiResults() {
		if len(res.Data) == 0 {
			continue
		}
		if err := p.ctx.Storage.StoreResults(context.Background(), res.Data, res.CollectionName); err != nil {
			p.ctx.Logger.Printf("[network-latency] store error for %s: %v", res.CollectionName, err)
		}
	}
	if data, coll := p.ep.GetResults(); len(data) > 0 {
		return p.ctx.Storage.StoreResults(context.Background(), data, coll)
	}
	return nil
}

func init() { pluginloader.Register("network-latency", func() pluginsdk.Plugin { return &Plugin{} }) }
