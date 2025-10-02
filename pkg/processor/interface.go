package processor

import "github.com/bft-labs/cometbft-analyzer-types/pkg/events"

// EventProcessor defines the behaviour for processing events.
type EventProcessor interface {
	Process(events.Event) error
}

// ResultsCollector defines the behaviour for collecting results from processors.
type ResultsCollector interface {
	GetResults() ([]interface{}, string) // returns (results, collection_name)
}

// MultiCollectionResultsCollector defines the behaviour for processors that return results to multiple collections.
type MultiCollectionResultsCollector interface {
	GetMultiResults() []ProcessorResult // returns multiple results with different collection names
}
