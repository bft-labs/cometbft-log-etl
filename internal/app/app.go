package app

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/bft-labs/cometbft-analyzer-types/pkg/events"
	"github.com/bft-labs/cometbft-log-etl/internal/converter"
	"github.com/bft-labs/cometbft-log-etl/internal/parser"
	"github.com/bft-labs/cometbft-log-etl/types"
)

// Service orchestrates reading log files and converting to normalized events.
type Service struct{}

// NewService creates a Service.
func NewService() *Service { return &Service{} }

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
		parsedEvents, nodeID, valAddr, err := parseFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", filePath, err)
		}
		attachMetadata(parsedEvents, nodeID, valAddr)
		entireEvents = append(entireEvents, parsedEvents...)
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
	// Increase buffer to handle very long log lines (default is 64K).
	// Allocate a 1MB initial buffer with a generous 64MB max token size.
	buf := make([]byte, 0, 1<<20)
	scanner.Buffer(buf, 64<<20)
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

// (processing happens via plugins; no direct processor invocation here)
