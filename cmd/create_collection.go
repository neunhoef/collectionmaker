package cmd

import (
	"context"
	"errors"
	"github.com/arangodb/go-driver"
	"github.com/neunhoef/smart-graph-maker/pkg/database"
	err2 "github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var (
	cmdCreateCollection = &cobra.Command{
		Use:   "collection",
		Short: "Create collection with provided number of document and size of each document",
		RunE:  createCollection,
	}
)

func init() {
	var database, collection string
	var size, count int64
	var numberOfShards int

	cmdCreateCollection.Flags().Int64Var(&size, "size", 0, "Size (in bytes) of a collection")
	cmdCreateCollection.Flags().Int64Var(&count, "count", 0, "Number of documents")
	cmdCreateCollection.Flags().IntVar(&numberOfShards, "shards", 1, "Number of shards")
	cmdCreateCollection.Flags().StringVar(&database, "database", "_system",
		"Name of database which should be used")
	cmdCreateCollection.Flags().StringVar(&collection, "collection", "test",
		"Name of collection which should be used")
}

func createCollection(cmd *cobra.Command, _ []string) error {
	expectedSize, _ := cmd.Flags().GetInt64("size")
	expectedCount, _ := cmd.Flags().GetInt64("count")
	DBName, _ := cmd.Flags().GetString("database")
	colName, _ := cmd.Flags().GetString("collection")
	shards, _ := cmd.Flags().GetInt("shards")

	if expectedSize == 0 {
		return errors.New("file with the size should be provided --sizefile")
	}

	if expectedCount == 0 {
		return errors.New("file with the count should be provided --countfile")
	}

	DBHandle, err := database.CreateOrGetDatabase(context.Background(), _client, DBName, nil)
	if err != nil {
		return err2.Wrap(err, "can not create/get database")
	}

	options := driver.CreateCollectionOptions{
		NumberOfShards: shards,
	}

	colHandle, err := database.CreateOrGetCollection(context.Background(), DBHandle, colName, &options)
	if err != nil {
		return err2.Wrap(err, "can not create a collection")
	}

	creator := database.NewCollectionCreator(expectedSize, expectedCount, &database.DocumentWithOneField{}, colHandle)
	return creator.CreateDocuments(context.Background())
}
