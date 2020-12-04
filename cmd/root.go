package cmd

import (
	"github.com/arangodb/go-driver"
	"github.com/neunhoef/collectionmaker/pkg/client"
	"github.com/spf13/cobra"
)

var (
	cmdRoot = &cobra.Command{
		Short: "The collection maker is a tool for creating data from different sources.",
	}
	endpoints []string
	verbose   bool
	_client   driver.Client
	jwt       string
	username  string
	password  string
)

func init() {
	rootFlags := cmdRoot.PersistentFlags()
	rootFlags.StringSliceVar(&endpoints, "endpoint", []string{"http://localhost:8529"},
		"Endpoint of server where data should be written.")
	rootFlags.BoolVarP(&verbose, "verbose", "v", false, "Verbose output")
	rootFlags.StringVar(&jwt, "jwt", "", "Verbose output")
	rootFlags.StringVar(&username, "username", "root", "User name for database access.")
	rootFlags.StringVar(&password, "password", "", "Password for database access.")
}

func connect(_ *cobra.Command, _ []string) error {
	var err error

	if len(jwt) == 0 {
		_client, err = client.NewClient(endpoints, driver.BasicAuthentication(username, password))
	} else {
		_client, err = client.NewClient(endpoints, driver.RawAuthentication("bearer "+jwt))
	}

	return err
}

func Execute() error {
	return cmdRoot.Execute()
}
