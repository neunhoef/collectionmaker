package client

import (
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"github.com/pkg/errors"
)

// NewClient creates new client to the provided endpoints.
func NewClient(endpoints []string, auth driver.Authentication) (driver.Client, error) {

	for i, e := range endpoints {
		fmt.Printf("%d %s\n", i, e)
	}

	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: endpoints,
		ConnLimit: 64,
	})
	if err != nil {
		return nil, errors.Wrap(err, "could not create connection")
	}

	client, err := driver.NewClient(driver.ClientConfig{
		Connection:     conn,
		Authentication: auth,
	})
	if err != nil {
		return nil, errors.Wrap(err, "could not create client")
	}

	return client, nil
}
