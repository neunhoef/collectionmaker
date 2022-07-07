package cmd

import (
	"github.com/spf13/cobra"
)

var (
	cmdRead = &cobra.Command{
		Use:               "read",
		Short:             "Read resources.",
		PersistentPreRunE: connect,
	}
)

func init() {
	cmdRoot.AddCommand(cmdRead)
	cmdRead.AddCommand(cmdReadBatchImport)
}
