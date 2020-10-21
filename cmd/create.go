package cmd

import (
	"github.com/arangodb/go-driver"
	"github.com/neunhoef/collectionmaker/pkg/client"
	"github.com/spf13/cobra"
)

var (
	cmdCreate = &cobra.Command{
		Use:               "create",
		Short:             "Create resources (database, collection, graph) from different input data.",
		PersistentPreRunE: connect,
	}
	cmdTest = &cobra.Command{
		Use:               "test",
		Short:             "Test resources",
		PersistentPreRunE: connect,
	}
	_client driver.Client
)

func init() {
	cmdRoot.AddCommand(cmdCreate)
	cmdCreate.AddCommand(cmdCreateFromDebugScript)
	cmdCreate.AddCommand(cmdCreateCollection)
	cmdCreate.AddCommand(cmdCreateGraph)

	cmdRoot.AddCommand(cmdTest)
	cmdTest.AddCommand(cmdTestGraph)
}

func connect(_ *cobra.Command, _ []string) error {
	var err error

	_client, err = client.NewClient(endpoints, driver.BasicAuthentication("root", ""))
	return err
}
