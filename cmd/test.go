package cmd

import "github.com/spf13/cobra"

var (
	cmdTest = &cobra.Command{
		Use:               "test",
		Short:             "Test resources",
		PersistentPreRunE: connect,
	}
)

func init() {
	cmdRoot.AddCommand(cmdTest)
}
