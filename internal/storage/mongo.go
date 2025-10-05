package storage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/bft-labs/cometbft-analyzer-types/pkg/events"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoStorage handles all MongoDB operations.
type MongoStorage struct {
	client *mongo.Client
	dbName string
}

// NewMongoStorage creates a new MongoDB storage service.
func NewMongoStorage(ctx context.Context, uri string) (*MongoStorage, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("mongo connect: %w", err)
	}

	dbName, err := generateDBName()
	if err != nil {
		return nil, fmt.Errorf("generate db name: %w", err)
	}

	return &MongoStorage{
		client: client,
		dbName: dbName,
	}, nil
}

// NewMongoStorageWithSimulation creates a new MongoDB storage service with a specific simulation ID.
func NewMongoStorageWithSimulation(ctx context.Context, uri string, simulationID string) (*MongoStorage, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("mongo connect: %w", err)
	}

	return &MongoStorage{
		client: client,
		dbName: simulationID,
	}, nil
}

// Close disconnects from MongoDB.
func (s *MongoStorage) Close(ctx context.Context) error {
	return s.client.Disconnect(ctx)
}

// GetEventsStoreFunc returns a function to store events.
func (s *MongoStorage) GetEventsStoreFunc(ctx context.Context) func([]events.Event) error {
	eventsColl := s.client.Database(s.dbName).Collection("events")
	return func(evts []events.Event) error {
		docs := make([]interface{}, len(evts))
		for i, evt := range evts {
			docs[i] = evt
		}
		return s.bulkInsert(ctx, eventsColl, docs, 1000)
	}
}

// StoreResults stores processor results in the appropriate collection.
func (s *MongoStorage) StoreResults(ctx context.Context, results []interface{}, collectionName string) error {
	if len(results) == 0 {
		return nil
	}

	collection := s.client.Database(s.dbName).Collection(collectionName)
	return s.bulkInsert(ctx, collection, results, 1000)
}

// bulkInsert performs batch insertion of documents into a MongoDB collection.
func (s *MongoStorage) bulkInsert(ctx context.Context, collection *mongo.Collection, docs []interface{}, batchSize int) error {
	n := len(docs)
	for i := 0; i < n; i += batchSize {
		end := i + batchSize
		if end > n {
			end = n
		}
		batch := make([]interface{}, end-i)
		for j := i; j < end; j++ {
			batch[j-i] = docs[j]
		}
		if _, err := collection.InsertMany(ctx, batch, options.InsertMany().SetOrdered(false)); err != nil {
			return err
		}
	}
	return nil
}

// generateDBName returns a string like "cometbft_sim_20250628T163045_ab12f3c4".
func generateDBName() (string, error) {
	const prefix = "cometbft_sim_"
	// ISO-like timestamp: 20250628T163045
	timestamp := time.Now().Format("20060102T150405")

	// Create a random 4-byte hex string
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	randHex := hex.EncodeToString(b)

	return fmt.Sprintf("%s%s_%s", prefix, timestamp, randHex), nil
}
