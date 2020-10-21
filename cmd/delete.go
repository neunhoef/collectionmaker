package cmd

import (
	"errors"
	"github.com/spf13/cobra"
)

var (
	cmdDelete = &cobra.Command{
		Use:               "delete",
		Short:             "Delete resources",
		PersistentPreRunE: connect,
	}
	cmdDeleteDatabases = &cobra.Command{
		Use:   "database",
		Short: "Delete database",
		RunE:  deleteDatabases,
	}
)

func init() {
	cmdRoot.AddCommand(cmdDelete)

	cmdDelete.AddCommand(cmdDeleteDatabases)
}

func deleteDatabases(cmd *cobra.Command, _ []string) error {
	return errors.New("it is not implemented yet")
	//DBHandles, err := _client.Databases(context.Background())
	//if err != nil {
	//	return err2.Wrap(err, "can not create/get database")
	//}
	//
	//for _, DBHandle := range DBHandles {
	//	if database.IsNameSystemReserved(DBHandle.Name()) {
	//		continue
	//	}
	//	if err := DBHandle.Remove(context.Background()); err != nil {
	//		return err
	//	}
	//}
	//return nil
}
