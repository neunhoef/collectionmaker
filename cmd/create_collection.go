package cmd

import (
	"bufio"
	"context"
	"errors"
	"github.com/arangodb/go-driver"
	"github.com/neunhoef/smart-graph-maker/pkg/database"
	"github.com/spf13/cobra"
	"os"
)

var (
	cmdCreateCollection = &cobra.Command{
		Use:   "collection",
		Short: "Create collection with provided number of document and size of each document",
		RunE:  createCollection,
	}
	cmdCreateCollectionFile = &cobra.Command{
		Use:   "file",
		Short: "Create collection with provided number of document and size of each document",
		RunE:  createCollectionFromFile,
	}
)

func init() {
	var database, collection, file string
	var size, count int64
	var numberOfShards int

	cmdCreateCollection.Flags().Int64Var(&size, "size", 0, "Size (in bytes) of a collection")
	cmdCreateCollection.Flags().Int64Var(&count, "count", 0, "Number of documents")
	cmdCreateCollection.Flags().IntVar(&numberOfShards, "shards", 1, "Number of shards")
	cmdCreateCollection.Flags().StringVar(&database, "database", "_system",
		"Name of database which should be used")
	cmdCreateCollection.Flags().StringVar(&collection, "collection", "test",
		"Name of collection which should be used")

	cmdCreateCollection.AddCommand(cmdCreateCollectionFile)
	cmdCreateCollectionFile.Flags().StringVar(&file, "file", "",
		"File with details about the collection")
	cmdCreateCollectionFile.Flags().IntVar(&numberOfShards, "shards", 1, "Number of shards")
	cmdCreateCollectionFile.Flags().StringVar(&database, "database", "_system",
		"Name of database which should be used")
	cmdCreateCollectionFile.Flags().StringVar(&collection, "collection", "test",
		"Name of collection which should be used")
}

func createCollectionFromFile(cmd *cobra.Command, _ []string) error {
	filename, _ := cmd.Flags().GetString("file")
	DBName, _ := cmd.Flags().GetString("database")
	colName, _ := cmd.Flags().GetString("collection")
	shards, _ := cmd.Flags().GetInt("shards")

	options := driver.CreateCollectionOptions{
		NumberOfShards: shards,
	}

	colHandle, err := database.CreateOrGetDatabaseCollection(context.Background(), _client, DBName, colName, &options)
	if err != nil {
		return err
	}

	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanWords)

	creator := database.NewCollectionCreator(&database.DocumentsFromFile{
		Scanner: scanner,
	}, colHandle)

	return creator.CreateDocuments(context.Background())
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

	options := driver.CreateCollectionOptions{
		NumberOfShards: shards,
	}

	colHandle, err := database.CreateOrGetDatabaseCollection(context.Background(), _client, DBName, colName, &options)
	if err != nil {
		return err
	}

	creator := database.NewCollectionCreator(&database.DocumentsWithEqualLength{
		ExpectedSize:  expectedSize,
		ExpectedCount: expectedCount,
	}, colHandle)

	return creator.CreateDocuments(context.Background())
}
