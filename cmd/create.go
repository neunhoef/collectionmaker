package cmd

import (
	"github.com/spf13/cobra"
)

var (
	cmdCreate = &cobra.Command{
		Use:               "create",
		Short:             "Create resources (database, collection, graph) from different input data.",
		PersistentPreRunE: connect,
	}
)

func init() {
	cmdRoot.AddCommand(cmdCreate)
	cmdCreate.AddCommand(cmdCreateFromDebugScript)
	cmdCreate.AddCommand(cmdCreateCollection)
	cmdCreate.AddCommand(cmdCreateGraph)
	cmdCreate.AddCommand(cmdCreateEdgeCol)
	cmdCreate.AddCommand(cmdCreateGraphCols)
	cmdCreate.AddCommand(cmdCreateSmartGraphConnectedComponents)
	cmdCreate.AddCommand(cmdCreateBatchImport)
}
