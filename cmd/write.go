package cmd

import (
	"github.com/spf13/cobra"
)

var (
	cmdWrite = &cobra.Command{
		Use:               "write",
		Short:             "Write resources (edges).",
		PersistentPreRunE: connect,
	}
)

func init() {
	cmdRoot.AddCommand(cmdWrite)
	cmdWrite.AddCommand(cmdWriteEdges)
	cmdWrite.AddCommand(cmdElCheapoWrites)
	cmdWrite.AddCommand(cmdWriteGraph)
}
