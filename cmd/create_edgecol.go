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

	cmdCreateEdgeCol.Flags().BoolVar(&drop, "drop", drop, "set -drop to true to drop data before start")
}

func createEdgeCol(cmd *cobra.Command, _ []string) error {
	drop, _ := cmd.Flags().GetBool("drop")
	db, err := _client.Database(context.Background(), "_system")
	if err != nil {
		return errors.Wrapf(err, "can not get database: %s", "_system")
	}

	if err := setupEdgeCol(drop, db); err != nil {
		return errors.Wrapf(err, "can not create edge collection")
	}

	return nil
}

// setupEdgeCol will set up a single edge collecion
func setupEdgeCol(drop bool, db driver.Database) error {
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
			NumberOfShards: 42,
			ReplicationFactor: 3,
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
