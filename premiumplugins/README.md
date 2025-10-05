Premium Plugins (Private)

This directory documents where premium or custom plugins would integrate.

- Build your premium plugins in a separate, private module/repo.
- Each plugin should import `github.com/bft-labs/cometbft-log-etl/pkg/pluginloader` and call `pluginloader.Register("your-plugin-name", factory)` in an `init()`.
- Premium binaries can be linked by including the module as a dependency or through a private build step; this OSS repo never includes premium code.
- If `CV_PRO_ENABLED=true` is set but a configured premium plugin is not found, the app logs a warning and continues normally.

Example stub (do not commit real code here):

```go
package mypremium

import (
    "github.com/bft-labs/cometbft-log-etl/pkg/pluginloader"
    "github.com/bft-labs/cometbft-log-etl/pkg/pluginsdk"
)

type Plugin struct{}

func (p *Plugin) Name() string { return "anomaly-detection" }
func (p *Plugin) Init(ctx pluginsdk.Context) error { return nil }
func (p *Plugin) Process(event interface{}) error { return nil }
func (p *Plugin) Finalize() error { return nil }

func init() { pluginloader.Register("anomaly-detection", func() pluginsdk.Plugin { return &Plugin{} }) }
```

