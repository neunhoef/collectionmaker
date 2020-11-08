package cmd

import (
	"context"
	"errors"
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/util"
	"github.com/neunhoef/collectionmaker/pkg/client"
	err2 "github.com/pkg/errors"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
	"path"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	cmdTestChecksum = &cobra.Command{
		Use:   "checksum",
		Short: "Test databases consistency",
		RunE:  testChecksums,
	}
	systemCollectionThatRequireSync = []string{
		"_appbundles",
		"_apps",
		"_aqlfunctions",
		"_graphs",
		"_users",
		"_analyzers",
		// Special cases
		"_jobs",
		"_queues",
	}
	smartGraphCollectionPrefixesThatRequireSync = []string{
		"_local_",
		"_to_",
		// Leave out _from_
	}
	systemCollectionThatRequireRename = map[string]string{
		"_jobs":   "_jobsbackup",
		"_queues": "_queuesbackup",
	}
)

func init() {
	var endpointTarget, jwtTarget, database string
	cmdTest.AddCommand(cmdTestChecksum)

	flags := cmdTestChecksum.PersistentFlags()
	flags.StringVar(&endpointTarget, "endpoint-target", "", "Endpoint of target server")
	flags.StringVar(&jwtTarget, "jwt-target", "", "Verbose output")
	flags.StringVar(&database, "database", "", "Check only chosen database")
}

type ShardChecksum struct {
	shardID  driver.ShardID
	checksum string
	err      error
}

type ShardsChecksum []ShardChecksum
type ByShardID ShardsChecksum

func (s ByShardID) Len() int           { return len(s) }
func (s ByShardID) Less(i, j int) bool { return s[i].shardID < s[j].shardID }
func (s ByShardID) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type CollectionChecksum struct {
	database   string
	err        error
	isSource   bool
	collection string
	checksums  ShardsChecksum
}

func (current CollectionChecksum) checksumStatus(other CollectionChecksum) (int, int, []string) {

	var source, target CollectionChecksum
	var output []string
	var OK, notOK int

	if current.isSource == other.isSource {
		return 0, 1, []string{fmt.Sprintf("ERROR it is a developer mistake\n %v\n, %v\n", current, other)}
	}

	if current.isSource {
		source = current
		target = other
	} else {
		source = other
		target = current
	}

	if source.err != nil || target.err != nil {
		return 0, 1, []string{fmt.Sprintf("ERROR %s\n\tsource: %s\nttarget: %s", source.database,
			source.err.Error(), target.err.Error())}
	}

	if len(source.checksums) != len(target.checksums) {
		return 0, 1, []string{fmt.Sprintf(
			"ERROR the collection '%s.%s' has different number of shards, source: %d, target: %d",
			source.database, source.collection, len(source.checksums), len(target.checksums))}
	}

	for i := range source.checksums {
		if source.checksums[i].err != nil || target.checksums[i].err != nil {
			if source.checksums[i].err != nil && target.checksums[i].err != nil {
				output = append(output, fmt.Sprintf("ERROR %s.%s\n\tsource: %s\n\ttarget: %s", source.database,
					source.collection, source.checksums[i].err.Error(), target.checksums[i].err.Error()))
			} else if source.checksums[i].err != nil {
				output = append(output, fmt.Sprintf("ERROR %s.%s\n\tsource: %s\n\ttarget: %s", source.database,
					source.collection, source.checksums[i].err.Error(), target.checksums[i].checksum))
			} else {
				output = append(output, fmt.Sprintf("ERROR %s.%s\n\tsource: %s\n\ttarget: %s", source.database,
					source.collection, source.checksums[i].checksum, target.checksums[i].err.Error()))
			}
			notOK++
			continue
		}

		if isChecksumEqual(source.checksums[i].checksum, target.checksums[i].checksum) {
			if verbose {
				output = append(output, fmt.Sprintf("OK %s.%s, source: %s, target: %s", source.database,
					source.collection, source.checksums[i].checksum, target.checksums[i].checksum))
			}
			OK++
			continue
		}

		notOK++
		output = append(output, fmt.Sprintf("ERROR %s.%s\n\tsource: %s\n\ttarget: %s", source.database,
			source.collection, source.checksums[i].checksum, target.checksums[i].checksum))
	}

	return OK, notOK, output
}
func testChecksums(cmd *cobra.Command, _ []string) error {
	endpointTarget, _ := cmd.Flags().GetString("endpoint-target")
	jwtTarget, _ := cmd.Flags().GetString("jwt-target")
	database, _ := cmd.Flags().GetString("database")

	if len(jwt) == 0 {
		return fmt.Errorf("--jwt must be provided for the source data center")
	}

	targetAuth := driver.RawAuthentication("bearer " + jwtTarget)
	targetClient, err := client.NewClient([]string{endpointTarget}, targetAuth)
	if err != nil {
		return err
	}

	resultChannel := make(chan CollectionChecksum)
	g, ctxGroup := errgroup.WithContext(context.Background())
	ctxInterrupt, cancelInterrupt := context.WithCancel(context.Background())

	g.Go(func() error {
		return getChecksums(ctxInterrupt, _client, resultChannel, true, driver.RawAuthentication("bearer "+jwt), database)
	})

	g.Go(func() error {
		return getChecksums(ctxInterrupt, targetClient, resultChannel, false, targetAuth, database)
	})

	databases := make(map[string]map[string]CollectionChecksum)
	printStatus := make([]string, 0)
	var counterOK, counterErrors int
	go func() {
		var finished bool
		for {
			select {
			case result := <-resultChannel:
				finished = false
				if _, ok := databases[result.database]; !ok {
					databases[result.database] = make(map[string]CollectionChecksum)
				}

				current, ok := databases[result.database][result.collection]
				if !ok {
					databases[result.database][result.collection] = result
					continue
				}

				OK, notOK, output := current.checksumStatus(result)
				if len(output) > 0 {
					printStatus = append(printStatus, output...)
				}
				counterOK += OK
				counterErrors += notOK
				delete(databases[result.database], result.collection)
				fmt.Printf("\rProgress OK: %d, errors: %d", counterOK, counterErrors)
			case <-time.After(time.Second * 1):
				if finished {
					cancelInterrupt()
					fmt.Println()
					return
				}
				if ctxGroup.Err() != nil {
					finished = true
				}
			}
		}
	}()

	if err := g.Wait(); err != nil {
		cancelInterrupt()
		fmt.Println(err)
		return err
	}

	<-ctxInterrupt.Done()

	for _, status := range printStatus {
		fmt.Println(status)
	}
	for DBName, database := range databases {
		for collectionName, collection := range database {
			counterErrors++
			if collection.isSource {
				fmt.Printf("ERROR the collection '%s.%s' does not exist on the target data center\n",
					DBName, collectionName)
			} else {
				fmt.Printf("ERROR the collection '%s.%s' does not exist on the source data center\n",
					DBName, collectionName)
			}
		}
	}
	fmt.Printf("OK: %d, errors: %d\n", counterOK, counterErrors)

	return nil
}

func isChecksumEqual(checksum1, checksum2 string) bool {
	for _, c := range checksum1 {
		if c < '0' || c > '9' {
			return false
		}
	}
	for _, c := range checksum2 {
		if c < '0' || c > '9' {
			return false
		}
	}
	return checksum1 == checksum2
}

func getChecksums(ctx context.Context, cl driver.Client, result chan<- CollectionChecksum, isSource bool,
	auth driver.Authentication, databaseFilter string) error {

	DBHandles, err := cl.Databases(ctx)
	if err != nil {
		return err2.Wrap(err, "can not create/get database")
	}

	clusterClient, err := cl.Cluster(ctx)
	if err != nil {
		return err2.Wrap(err, "can not get cluster client")
	}

	health, err := clusterClient.Health(ctx)
	if err != nil {
		return err2.Wrap(err, "can not get cluster health")
	}

	DBServers := make(map[driver.ServerID]driver.Client)
	for id, svr := range health.Health {
		if svr.Role == driver.ServerRoleDBServer {
			DBServerClient, err := client.NewClient([]string{util.FixupEndpointURLScheme(svr.Endpoint)}, auth)
			if err != nil {
				return err2.Wrap(err, "can not get DBServer clients")
			}
			DBServers[id] = DBServerClient
		}
	}

	wg := sync.WaitGroup{}
	throttle := make(chan struct{}, 30)
	for _, DBHandle := range DBHandles {
		if ctx.Err() != nil {
			break
		}
		DBName := DBHandle.Name()
		if len(databaseFilter) > 0 && DBName != databaseFilter {
			continue
		}

		inventory, err := clusterClient.DatabaseInventory(context.Background(), DBHandle)
		if err != nil {
			result <- CollectionChecksum{
				database: DBName,
				isSource: isSource,
				err:      err,
			}
			continue
		}

		for _, collection := range inventory.Collections {
			collection := collection
			if !MustSynchronizeShards(collection) {
				continue
			}

			if isSource {
				if collection.Parameters.Name != collectionNameException(collection.Parameters.Name) {
					// source data center can not have collections like "_jobsbackup" and "_queuesbackup"
					continue
				}
			} else if _, ok := systemCollectionThatRequireRename[collection.Parameters.Name]; ok {
				continue
			}

			if ctx.Err() != nil {
				break
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				throttle <- struct{}{}
				defer func() {
					<-throttle // TODO it should be per DBserver
				}()

				//var timeout time.Duration
				//if col, err := DBHandle.Collection(context.Background(), collection.Parameters.Name); err == nil {
				//	if count, err := col.Count(context.Background()); err == nil {
				//		timeout = time.Duration(int(count)/len(collection.Parameters.Shards)) * (time.Microsecond * 10)
				//	}
				//}
				//
				//if timeout < time.Minute*5 {
				//	timeout = time.Minute * 5
				//} else if timeout > time.Hour {
				//	timeout = time.Hour
				//}

				checksums := make(ShardsChecksum, 0, len(collection.Parameters.Shards))
				for shardID, shardDBServers := range collection.Parameters.Shards {

					shardResult := ShardChecksum{
						shardID: shardID,
					}
					if len(shardDBServers) > 0 {
						connection := DBServers[shardDBServers[0]].Connection()
						ctxGetChecksum, cancel := context.WithTimeout(ctx, time.Minute*30)
						shardResult.checksum, shardResult.err = getChecksum(ctxGetChecksum, connection, DBName, string(shardID))
						cancel()
						if ctx.Err() != nil {
							return
						}
					} else {
						shardResult.err = errors.New("no DB server")
					}

					checksums = append(checksums, shardResult)
				}

				sort.Sort(ByShardID(checksums))
				result <- CollectionChecksum{
					database:   DBName,
					isSource:   isSource,
					collection: collectionNameException(collection.Parameters.Name),
					checksums:  checksums,
				}
			}()
		}
	}
	wg.Wait()

	return nil
}

func getChecksum(ctx context.Context, connection driver.Connection, DBName, shardID string) (string, error) {

	req, err := connection.NewRequest("GET",
		path.Join("_db", DBName, "_api/collection", shardID, "checksum"))
	if err != nil {
		return "", err2.Wrap(err, "can not create request to fetch checksum")
	}

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

func MustSynchronizeShards(col driver.InventoryCollection) bool {
	if !col.Parameters.IsSystem {
		if col.Parameters.Type == driver.CollectionTypeEdge && col.Parameters.IsSmart {
			// This is the case of a "virtual" smart edge collection, we do not
			// synchronize this, but rather the two hidden system collections
			// with the prefix "_local_" and "_to_".
			return false
		}
		return true
	}
	for _, x := range systemCollectionThatRequireSync {
		if col.Parameters.Name == x {
			return true
		}
	}
	for _, prefix := range smartGraphCollectionPrefixesThatRequireSync {
		if strings.HasPrefix(col.Parameters.Name, prefix) {
			return true
		}
	}

	if col.Parameters.Name == "_queuesbackup" || col.Parameters.Name == "_jobsbackup" {
		return true
	}

	return false
}
