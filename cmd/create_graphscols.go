package cmd

import (
	"context"
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var (
	cmdCreateGraphCols = &cobra.Command{
		Use:   "graphcols",
		Short: "Create collections for a graph",
		RunE:  createGraphCols,
	}
)

func init() {
	var drop = false
	var replicationFactor int
	var numberOfShards int

	cmdCreateGraphCols.Flags().BoolVar(&drop, "drop", drop, "set -drop to true to drop data before start")
	cmdCreateGraphCols.Flags().IntVar(&replicationFactor, "replicationFactor", 3, "replication factor for edge collection")
	cmdCreateGraphCols.Flags().IntVar(&numberOfShards, "numberOfShards", 42, "number of shards of edge collection")
}

func createGraphCols(cmd *cobra.Command, _ []string) error {
	drop, _ := cmd.Flags().GetBool("drop")
	db, err := _client.Database(context.Background(), "_system")
	if err != nil {
		return errors.Wrapf(err, "can not get database: %s", "_system")
	}

	if err := setupGraphVertexCol(cmd, drop, db); err != nil {
		return errors.Wrapf(err, "can not create graph vertex collection")
	}
	if err := setupGraphEdgeCol(cmd, drop, db); err != nil {
		return errors.Wrapf(err, "can not create graph edge collection")
	}

	return nil
}

// setupGraphVertexCol will set up a single vertex collecion
func setupGraphVertexCol(cmd *cobra.Command, drop bool, db driver.Database) error {
	replicationFactor, _ := cmd.Flags().GetInt("replicationFactor")
	numberOfShards, _ := cmd.Flags().GetInt("numberOfShards")

	ec, err := db.Collection(nil, "instances")
	if err == nil {
		if !drop {
			fmt.Printf("Found vertex collection 'instances' already, setup is already done.\n")
			return nil
		}
		err = ec.Remove(nil)
		if err != nil {
			fmt.Printf("Could not drop vertex collection 'instances': %v\n", err)
			return err
		}
	} else if !driver.IsNotFound(err) {
		fmt.Printf("Error: could not look for vertex collection 'instances': %v\n", err)
		return err
	}

	// Now create the vertex collection:
	_, err = db.CreateCollection(nil, "instances", &driver.CreateCollectionOptions{
			Type: driver.CollectionTypeDocument,
			NumberOfShards: numberOfShards,
			ReplicationFactor: replicationFactor,
	})
	if err != nil {
		fmt.Printf("Error: could not create vertex collection 'instances': %v\n", err)
		return err
	}
	return nil
}

// setupGraphEdgeCol will set up a single edge collecion
func setupGraphEdgeCol(cmd *cobra.Command, drop bool, db driver.Database) error {
	replicationFactor, _ := cmd.Flags().GetInt("replicationFactor")
	numberOfShards, _ := cmd.Flags().GetInt("numberOfShards")

	ec, err := db.Collection(nil, "steps")
	if err == nil {
		if !drop {
			fmt.Printf("Found edge collection 'steps' already, setup is already done.\n")
			return nil
		}
		err = ec.Remove(nil)
		if err != nil {
			fmt.Printf("Could not drop edge collection 'steps': %v\n", err)
			return err
		}
	} else if !driver.IsNotFound(err) {
		fmt.Printf("Error: could not look for edge collection 'steps': %v\n", err)
		return err
	}

	// Now create the edge collection:
	_, err = db.CreateCollection(nil, "steps", &driver.CreateCollectionOptions{
			Type: driver.CollectionTypeEdge,
			NumberOfShards: numberOfShards,
			ReplicationFactor: replicationFactor,
	})
	if err != nil {
		fmt.Printf("Error: could not create edge collection 'steps': %v\n", err)
		return err
	}
	return nil
}
