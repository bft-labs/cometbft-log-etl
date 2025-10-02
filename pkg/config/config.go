package config

import (
	"flag"
	"fmt"
)

// Config holds application configuration.
type Config struct {
	Directory    string
	MongoURI     string
	SimulationID string
}

// LoadFromFlags loads configuration from command line flags.
func LoadFromFlags() (*Config, error) {
	var dirname string
	var mongoURI string
	var simulationID string

	flag.StringVar(&dirname, "dir", "", "Directory path containing JSON logs from multiple nodes")
	flag.StringVar(&mongoURI, "mongo-uri", "mongodb://localhost:27017", "MongoDB connection URI")
	flag.StringVar(&simulationID, "simulation", "", "Simulation ID for database naming")
	flag.Parse()

	if dirname == "" {
		return nil, fmt.Errorf("please provide a directory path using the -dir flag")
	}

	if simulationID == "" {
		return nil, fmt.Errorf("please provide a simulation ID using the -simulation flag")
	}

	return &Config{
		Directory:    dirname,
		MongoURI:     mongoURI,
		SimulationID: simulationID,
	}, nil
}
