package cmd

import (
	"context"
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/util"
	"github.com/neunhoef/collectionmaker/pkg/client"
	err2 "github.com/pkg/errors"
	"github.com/spf13/cobra"
	"path"
	"sort"
	"sync"
	"time"
)

var (
	cmdTestChecksum = &cobra.Command{
		Use:   "checksum",
		Short: "Test databases consistency",
		RunE:  testChecksums,
	}
)

func init() {
	var endpointTarget, jwtTarget string
	cmdTest.AddCommand(cmdTestChecksum)

	flags := cmdTestChecksum.PersistentFlags()
	flags.StringVar(&endpointTarget, "endpoint-target", "", "Endpoint of target server")
	flags.StringVar(&jwtTarget, "jwt-target", "", "Verbose output")
}

func testChecksums(cmd *cobra.Command, _ []string) error {
	endpointTarget, _ := cmd.Flags().GetString("endpoint-target")
	jwtTarget, _ := cmd.Flags().GetString("jwt-target")

	wg := sync.WaitGroup{}
	wg.Add(2)
	var errSource, errTarget error
	var sourceChecksums, targetChecksums DatabasesChecksums

	if len(jwt) == 0 {
		return fmt.Errorf("--jwt must be provided for the source data center")
	}
	go func() {
		defer wg.Done()
		sourceChecksums, errSource = getChecksums(_client, driver.RawAuthentication("bearer "+jwt))
	}()

	go func() {
		defer wg.Done()
		var targetClient driver.Client

		auth := driver.RawAuthentication("bearer " + jwtTarget)
		targetClient, errTarget = client.NewClient([]string{endpointTarget}, auth)
		if errTarget != nil {
			return
		}
		targetChecksums, errTarget = getChecksums(targetClient, auth)
	}()

	wg.Wait()

	if errSource != nil {
		return err2.Wrapf(errSource, "can not fetch checksums from source data center")
	}

	if errTarget != nil {
		return err2.Wrapf(errTarget, "can not fetch checksums from source data center")
	}

	var errorCounter int
	for DBName, database := range sourceChecksums {
		targetDB, ok := targetChecksums[DBName]
		if !ok {
			errorCounter++
			fmt.Printf("ERROR the database '%s' does not exist on the target data center\n", DBName)
			continue
		}

		for collectionName, collection := range database {
			targetCollection, ok := targetDB[collectionName]
			if !ok {
				errorCounter++
				fmt.Printf("ERROR the collection '%s.%s' does not exist on the target data center\n",
					DBName, collectionName)
				continue
			}

			if len(collection) != len(targetCollection) {
				errorCounter++
				fmt.Printf(
					"ERROR the collection '%s.%s' has different number of shards, source: %d, target: %d\n",
					DBName, collectionName, len(collection), len(targetCollection))
				continue
			}

			for i := range collection {
				if verbose {
					fmt.Printf("OK %s.%s, source: %s, target: %s\n",
						DBName, collectionName, collection[i], targetCollection[i])
				} else {
					if collection[i] != targetCollection[i] {
						errorCounter++
						fmt.Printf("ERROR '%s.%s', source: %s, target: %s\n",
							DBName, collectionName, collection[i], targetCollection[i])
					}
				}
			}
		}
	}

	for DBName, database := range targetChecksums {
		sourceDB, ok := sourceChecksums[DBName]
		if !ok {
			errorCounter++
			fmt.Printf("ERROR database '%s' does not exist on the source data center\n", DBName)
			continue
		}

		for collectionName := range database {
			_, ok := sourceDB[collectionName]
			if !ok {
				errorCounter++
				fmt.Printf("ERROR collection '%s.%s' does not exist on the source data center\n", DBName, collectionName)
				continue
			}
		}
	}

	if !verbose && errorCounter == 0 {
		fmt.Println("The whole data is the same")
	}
	return nil
}

type DatabasesChecksums map[string]map[string][]string

func getChecksums(cl driver.Client, auth driver.Authentication) (DatabasesChecksums, error) {
	databasesChecksums := DatabasesChecksums{}
	DBHandles, err := cl.Databases(context.Background())
	if err != nil {
		return nil, err2.Wrap(err, "can not create/get database")
	}

	clusterClient, err := cl.Cluster(context.Background())
	if err != nil {
		return nil, err2.Wrap(err, "can not get cluster client")
	}

	health, err := clusterClient.Health(context.Background())
	if err != nil {
		return nil, err2.Wrap(err, "can not get cluster health")
	}

	DBServers := make(map[driver.ServerID]driver.Client)
	for id, svr := range health.Health {
		if svr.Role == driver.ServerRoleDBServer {
			//driver.RawAuthentication("bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhcmFuZ29kYiIsInNlcnZlcl9pZCI6ImZvbyJ9._gOqt6MEsrcYkGIv0hyh9sqSnM-8VjUho-0tjBoeI0Q"))
			DBServerClient, err := client.NewClient([]string{util.FixupEndpointURLScheme(svr.Endpoint)}, auth)
			if err != nil {
				return nil, err2.Wrap(err, "can not get DBServer clients")
			}
			DBServers[id] = DBServerClient
		}
	}

	wg := sync.WaitGroup{}
	var mutex sync.Mutex
	for _, DBHandle := range DBHandles {
		DBName := DBHandle.Name()
		if DBName != "coreBelgium5AMLD" {
			continue
		}
		inventory, err := clusterClient.DatabaseInventory(context.Background(), DBHandle)
		if err != nil {
			fmt.Println(err)
			continue
			//return nil, err2.Wrap(err, "can not get database inventory")
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			DBMap := make(map[string][]string)
			for _, collection := range inventory.Collections {
				array := make([]string, 0)
				shardMap := make(map[string]string)
				for shardID, shardDBServers := range collection.Parameters.Shards {
					array = append(array, string(shardID))

					if len(shardDBServers) == 0 {
						shardMap[string(shardID)] = "NO DBSERVER"
						continue
					}

					connection := DBServers[shardDBServers[0]].Connection()
					if checksum, err := getChecksum(connection, DBName, string(shardID)); err == nil {
						shardMap[string(shardID)] = checksum
					} else {
						shardMap[string(shardID)] = "ERROR" + err.Error()
					}
				}

				ChecksumArray := make([]string, 0)
				sort.Strings(array)
				for _, s := range array {
					if v, ok := shardMap[s]; ok {
						ChecksumArray = append(ChecksumArray, v)
					}
				}

				DBMap[collectionNameException(collection.Parameters.Name)] = ChecksumArray
			}

			mutex.Lock()
			databasesChecksums[DBName] = DBMap
			mutex.Unlock()
		}()
	}
	wg.Wait()

	return databasesChecksums, nil
}

func getChecksum(connection driver.Connection, DBName, shardID string) (string, error) {

	req, err := connection.NewRequest("GET",
		path.Join("_db", DBName, "_api/collection", shardID, "checksum"))
	if err != nil {
		return "", err2.Wrap(err, "can not create request to fetch checksum")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
	defer cancel()
	resp, err := connection.Do(ctx, req)
	if err != nil {
		return "", err2.Wrap(err, "can not get response for a checksum")
	}
	if err := resp.CheckStatus(200); err != nil {
		return "", err2.Wrap(err, "http status code should be 200")
	}

	var result struct {
		Checksum string `json:"checksum"`
	}
	if err := resp.ParseBody("", &result); err != nil {
		return "", err2.Wrap(err, "can not parse response for a checkusm")
	}

	return result.Checksum, nil
}

func collectionNameException(name string) string {
	if name == "_queuesbackup" {
		return "_queues"
	}

	if name == "_jobsbackup" {
		return "_jobs"
	}

	return name
}
