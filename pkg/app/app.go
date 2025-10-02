package app

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/bft-labs/cometbft-analyzer-types/pkg/events"
	"github.com/bft-labs/cometbft-log-etl/pkg/converter"
	"github.com/bft-labs/cometbft-log-etl/pkg/parser"
	"github.com/bft-labs/cometbft-log-etl/pkg/processor"
	"github.com/bft-labs/cometbft-log-etl/types"
)

// Service orchestrates reading log files and processing events.
// It can be extended with different processors.
type Service struct {
	Processors []processor.EventProcessor
}

// NewService creates a Service with the given processors.
func NewService(processors ...processor.EventProcessor) *Service {
	return &Service{Processors: processors}
}

// ParseDirectory scans the given directory for log files, converts them to events,
// and returns all events.
func (s *Service) ParseDirectory(ctx context.Context, dir string) ([]events.Event, error) {
	dirEntries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read dir %q: %w", dir, err)
	}
	var entireEvents []events.Event
	for _, entry := range dirEntries {
		fName := entry.Name()
		if entry.IsDir() || filepath.Ext(fName) != ".log" {
			continue
		}
		filePath := filepath.Join(dir, fName)
		events, nodeID, valAddr, err := parseFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", filePath, err)
		}
		attachMetadata(events, nodeID, valAddr)
		entireEvents = append(entireEvents, events...)
	}
	return entireEvents, nil
}

// parseFile reads a single file and returns parsed events along with the nodeID
// and validator address discovered in the logs.
func parseFile(filePath string) ([]events.Event, string, string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, "", "", err
	}
	defer f.Close()

	var (
		eventsList       []events.Event
		nodeID           string
		validatorAddress string
	)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		raw := make([]byte, len(scanner.Bytes()))
		copy(raw, scanner.Bytes())
		parsed, err := parser.Dispatch(raw)
		if err != nil {
			log.Printf("parse error: %v", err)
			continue
		}
		if parsed == nil {
			continue
		}
		if nodeID == "" {
			if id, ok := parsed.(*types.P2pNodeID); ok {
				nodeID = id.ID
			}
		}
		if validatorAddress == "" {
			if val, ok := parsed.(*types.Validator); ok {
				validatorAddress = val.Address
			}
		}
		evt, err := converter.Convert(parsed)
		if err != nil || evt == nil {
			log.Printf("convert error: %v", err)
			continue
		}
		eventsList = append(eventsList, evt.(events.Event))
	}
	if err := scanner.Err(); err != nil {
		log.Printf("scan error: %v", err)
	}
	if nodeID == "" || validatorAddress == "" {
		return nil, "", "", fmt.Errorf("node ID or validator address not found")
	}
	return eventsList, nodeID, validatorAddress, nil
}

func attachMetadata(eventsList []events.Event, nodeID, valAddr string) {
	for _, evt := range eventsList {
		evt.SetNodeId(nodeID)
		evt.SetValidatorAddress(valAddr)
	}
}

// ProcessEvents processes the events using all of the service's processors.
func (s *Service) ProcessEvents(events []events.Event) {
	for _, evt := range events {
		for _, proc := range s.Processors {
			if err := proc.Process(evt); err != nil {
				log.Printf("process error: %v", err)
			}
		}
	}
}
