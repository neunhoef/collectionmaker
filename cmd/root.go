package cmd

import (
	"github.com/spf13/cobra"
)

var (
	cmdRoot = &cobra.Command{
		Short: "The collection maker is a tool for creating data from different sources.",
	}
	endpoints []string
)

func init() {
	rootFlags := cmdRoot.PersistentFlags()
	rootFlags.StringSliceVar(&endpoints, "endpoint", []string{"http://localhost:8529"},
		"Endpoint of server where data should be written.")
}

func Execute() error {
	return cmdRoot.Execute()
}
