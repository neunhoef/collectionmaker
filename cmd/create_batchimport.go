package cmd

import (
	"context"
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var (
	cmdCreateBatchImport = &cobra.Command{
		Use:   "batchimport",
		Short: "Create batch import",
		RunE:  createBatchImport,
	}
)

func init() {
	var drop = false
	var replicationFactor int
	var numberOfShards int
	var collectionName string

	cmdCreateBatchImport.Flags().BoolVar(&drop, "drop", drop, "set -drop to true to drop data before start")
	cmdCreateBatchImport.Flags().IntVar(&replicationFactor, "replicationFactor", 3, "replication factor for edge collection")
	cmdCreateBatchImport.Flags().IntVar(&numberOfShards, "numberOfShards", 1, "number of shards of batch import collection")
	cmdCreateBatchImport.Flags().StringVar(&collectionName, "collection", "batchimport", "name of batch import collection")
}

func createBatchImport(cmd *cobra.Command, _ []string) error {
	drop, _ := cmd.Flags().GetBool("drop")
	db, err := _client.Database(context.Background(), "_system")
	if err != nil {
		return errors.Wrapf(err, "can not get database: %s", "_system")
	}

	if err := setupBatchImport(cmd, drop, db); err != nil {
		return errors.Wrapf(err, "can not create batch import collection")
	}

	return nil
}

// setupBatchImport will set up a single collecion for batch import
func setupBatchImport(cmd *cobra.Command, drop bool, db driver.Database) error {
	replicationFactor, _ := cmd.Flags().GetInt("replicationFactor")
	numberOfShards, _ := cmd.Flags().GetInt("numberOfShards")
	collectionName, _ := cmd.Flags().GetString("collection")

	ec, err := db.Collection(nil, collectionName)
	if err == nil {
		if !drop {
			fmt.Printf("Found batchimport collection already, setup is already done.\n")
			return nil
		}
		err = ec.Remove(nil)
		if err != nil {
			fmt.Printf("Could not drop batchimport collection: %v\n", err)
			return err
		}
	} else if !driver.IsNotFound(err) {
		fmt.Printf("Error: could not look for batchimport collection: %v\n", err)
		return err
	}

	// Now create the batchimport collection:
	_, err = db.CreateCollection(nil, collectionName, &driver.CreateCollectionOptions{
			Type: driver.CollectionTypeDocument,
			NumberOfShards: numberOfShards,
			ReplicationFactor: replicationFactor,
	})
	if err != nil {
		fmt.Printf("Error: could not create batchimport collection: %v\n", err)
		return err
	}
	return nil
}
