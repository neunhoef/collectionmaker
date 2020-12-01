package cmd

import (
	"context"
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var (
	cmdCreateEdgeCol = &cobra.Command{
		Use:   "edgecol",
		Short: "Create edgecol",
		RunE:  createEdgeCol,
	}
)

func init() {
	var drop = false
	var replicationFactor int
	var numberOfShards int

	cmdCreateEdgeCol.Flags().BoolVar(&drop, "drop", drop, "set -drop to true to drop data before start")
	cmdCreateEdgeCol.Flags().IntVar(&replicationFactor, "replicationFactor", 3, "replication factor for edge collection")
	cmdCreateEdgeCol.Flags().IntVar(&numberOfShards, "numberOfShards", 42, "number of shards of edge collection")
}

func createEdgeCol(cmd *cobra.Command, _ []string) error {
	drop, _ := cmd.Flags().GetBool("drop")
	db, err := _client.Database(context.Background(), "_system")
	if err != nil {
		return errors.Wrapf(err, "can not get database: %s", "_system")
	}

	if err := setupEdgeCol(cmd, drop, db); err != nil {
		return errors.Wrapf(err, "can not create edge collection")
	}

	return nil
}

// setupEdgeCol will set up a single edge collecion
func setupEdgeCol(cmd *cobra.Command, drop bool, db driver.Database) error {
	replicationFactor, _ := cmd.Flags().GetInt("replicationFactor")
	numberOfShards, _ := cmd.Flags().GetInt("numberOfShards")

	ec, err := db.Collection(nil, "edges")
	if err == nil {
		if !drop {
			fmt.Printf("Found edge collection already, setup is already done.\n")
			return nil
		}
		err = ec.Remove(nil)
		if err != nil {
			fmt.Printf("Could not drop edge collection: %v\n", err)
			return err
		}
	} else if !driver.IsNotFound(err) {
		fmt.Printf("Error: could not look for edge collection: %v\n", err)
		return err
	}

	// Now create the edge collection:
	edges, err := db.CreateCollection(nil, "edges", &driver.CreateCollectionOptions{
			Type: driver.CollectionTypeEdge,
			NumberOfShards: numberOfShards,
			ReplicationFactor: replicationFactor,
	})
	if err != nil {
		fmt.Printf("Error: could not create edge collection: %v\n", err)
		return err
	}
	_, _, err = edges.EnsurePersistentIndex(nil, []string{"fromUid"}, &driver.EnsurePersistentIndexOptions{
			Unique: false,
	})
	if err != nil {
		fmt.Printf("Error: could not create index on 'fromUid': %v\n", err)
		return err;
  }
	_, _, err = edges.EnsurePersistentIndex(nil, []string{"toUid"}, &driver.EnsurePersistentIndexOptions{
			Unique: false,
	})
	if err != nil {
		fmt.Printf("Error: could not create index on 'toUid': %v\n", err)
		return err;
  }
	_, _, err = edges.EnsurePersistentIndex(nil, []string{"score"}, &driver.EnsurePersistentIndexOptions{
			Unique: false,
	})
	if err != nil {
		fmt.Printf("Error: could not create index on 'score': %v\n", err)
		return err;
  }
	return nil
}
