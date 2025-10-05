package blockparts

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
		GetResults() ([]interface{}, string)
	}
}

func (p *Plugin) Name() string { return "block-parts" }

func (p *Plugin) Init(ctx pluginsdk.Context) error {
	p.ctx = ctx
	p.ep = NewProcessor(ctx.Ctx)
	return nil
}

func (p *Plugin) Process(event interface{}) error {
	if evt, ok := event.(analyzer.Event); ok {
		return p.ep.Process(evt)
	}
	return nil
}

func (p *Plugin) Finalize() error {
	data, coll := p.ep.GetResults()
	if len(data) > 0 {
		return p.ctx.Storage.StoreResults(context.Background(), data, coll)
	}
	return nil
}

func init() { pluginloader.Register("block-parts", func() pluginsdk.Plugin { return &Plugin{} }) }
