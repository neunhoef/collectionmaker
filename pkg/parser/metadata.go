package parser

import (
	"context"
	"errors"
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/neunhoef/smart-graph-maker/pkg/database"
)

// Databases all databases.
type Databases map[string]Database

// Database describes data for one database.
type Database struct {
	Collections map[string]Collection
}

// Collection describes data for one collection.
type Collection struct {
	Shards map[string]Shard //Shards int a collection with
}

// Shard describe information about one specific shard.
type Shard struct {
	Size              int // Size in bytes of a shard.
	Count             int // Count od documents of a shard.
	ReplicationFactor int
}

// ParseObject instructs how to parse one object from the source.
type ParseObject interface {
	GetObject(input string) (string, string, string, Shard, error)
}

// DatabaseMetaData contains metadata information about the system.
// The metadata can be fetched from different sources.
type DatabaseMetaData struct {
	collector Collector
	databases Databases
}

// Collector instructs how to build the metadata object from different sources.
type Collector interface {
	GetData() (Databases, error)
}

// NewDatabaseMetaData creates new metadata object.
func NewDatabaseMetaData(collector Collector) DatabaseMetaData {
	d := DatabaseMetaData{
		collector: collector,
	}

	return d
}

// Print prints collected data on the screen.
func (s *DatabaseMetaData) Print() {
	count := 0
	size := 0

	for d, database := range s.databases {
		for c, collection := range database.Collections {
			for s, shard := range collection.Shards {
				fmt.Printf("%s %s %s %d %d\n", d, c, s, shard.Count, shard.Size)
				count += shard.Count
				size += shard.Size
			}
		}
	}

	fmt.Printf("Size %d\n", size)
	fmt.Printf("Count %d\n", count)
}

// GetData fetches information about the databases.
func (s *DatabaseMetaData) GetData() error {
	databases, err := s.collector.GetData()
	if err != nil {
		return err
	}

	s.databases = databases

	return nil
}

func (c Collection) GetMetrics() (int64, int64) {
	size := 0
	count := 0

	for i := range c.Shards {
		size += c.Shards[i].Size
		count += c.Shards[i].Count
	}

	return int64(size), int64(count)
}

// CreateDatabases creates databases according to the source input.
func (s *DatabaseMetaData) CreateDatabases(ctx context.Context, client driver.Client,
	options *driver.CreateDatabaseOptions) error {

	var DBHandle driver.Database
	var colHandle driver.Collection
	var err error

	if client == nil {
		return errors.New("database client is not initialized")
	}

	for DBName, d := range s.databases {
		DBHandle, err = database.CreateOrGetDatabase(ctx, client, DBName, options)
		if err != nil {
			return err
		}

		for colName, collection := range d.Collections {
			if database.IsNameSystemReserved(colName) {
				continue
			}

			if len(collection.Shards) == 0 {
				continue
			}

			replicationFactor := 0
			for _, shard := range collection.Shards {
				replicationFactor = shard.ReplicationFactor
				break
			}
			options := driver.CreateCollectionOptions{
				ReplicationFactor: replicationFactor,
				NumberOfShards:    len(collection.Shards),
			}

			colHandle, err = database.CreateOrGetCollection(ctx, DBHandle, colName, &options)
			if err != nil {
				return err
			}

			expectedSize, expectedCount := collection.GetMetrics()
			creator := database.NewCollectionCreator(expectedSize, expectedCount, &database.DocumentWithOneField{}, colHandle)
			if err := creator.CreateDocuments(context.Background()); err != nil {
				return err
			}
		}
	}

	return nil
}
