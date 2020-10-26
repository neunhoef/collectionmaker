package cmd

import (
	"context"
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"math/rand"
	"sort"
	"time"
)

var (
	cmdTestGraph = &cobra.Command{
		Use:   "graph",
		Short: "Test graph",
		RunE:  testGraph,
	}
)

func init() {
	var runTimeSeconds int

	cmdTest.AddCommand(cmdTestGraph)
	cmdTestGraph.Flags().IntVar(&runTimeSeconds, "runTime", 30, "Run time in seconds")
	commonGraphFlags(cmdTestGraph)
}

type DurationSlice []time.Duration

func (d DurationSlice) Len() int {
	return len(d)
}

func (d DurationSlice) Less(a, b int) bool {
	return d[a] < d[b]
}

func (d DurationSlice) Swap(a, b int) {
	var dummy time.Duration = d[a]
	d[a] = d[b]
	d[b] = dummy
}

func testGraph(cmd *cobra.Command, _ []string) error {
	firstTenant, _ := cmd.Flags().GetInt("firstTenant")
	lastTenant, _ := cmd.Flags().GetInt("lastTenant")
	nrPathsPerTenant, _ := cmd.Flags().GetInt("nrPathsPerTenant")
	parallelism, _ := cmd.Flags().GetInt("parallelism")
	runTimeSeconds, _ := cmd.Flags().GetInt("runTime")

	db, err := _client.Database(context.Background(), "_system")
	if err != nil {
		return errors.Wrapf(err, "can not get database: %s", "_system")
	}

	return runRandomTest(db, runTimeSeconds, firstTenant, lastTenant, nrPathsPerTenant, parallelism)

}

func runRandomTest(db driver.Database, runTimeSeconds int, firstTenantNr int, lastTenantNr int,
	pathsPerTenant int, parallelism int) error {
	// parallelism ignored so far!
	startTime := time.Now()
	limit := time.Duration(runTimeSeconds) * time.Second
	for time.Now().Sub(startTime) < limit {
		// Run another 1000 random access queries:
		times := make([]time.Duration, 0, 1000)
		for i := 1; i <= 1000; i++ {
			start := time.Now()
			startVertex := fmt.Sprintf(
				"instances/ten%d:K%d",
				firstTenantNr+rand.Intn(lastTenantNr+1-firstTenantNr),
				rand.Intn(pathsPerTenant)+1)
			query := fmt.Sprintf(
				`FOR v, e IN 2..2 OUTBOUND "%s" GRAPH "G" RETURN v`,
				startVertex)
			cursor, err := db.Query(nil, query, nil)
			if err != nil {
				fmt.Printf("Error running query: %v\n", err)
				return err
			}
			count := 0
			for cursor.HasMore() {
				var vertex Instance
				_, err := cursor.ReadDocument(nil, &vertex)
				if err != nil {
					fmt.Printf("Error reading document from cursor: %v\n", err)
					return err
				}
				count = count + 1
			}
			cursor.Close()
			times = append(times, time.Now().Sub(start))
			if count != 1 {
				fmt.Printf("Got wrong count: %d, key: %s, query: %s\n", count, startVertex, query)
			}
			cursor.Close()
		}
		sort.Sort(DurationSlice(times))
		var sum int64 = 0
		for _, t := range times {
			sum = sum + int64(t)
		}
		fmt.Printf("Times for last 1000: %s (median), %s (90%%ile), %s (99%%ilie), %s (average)\n", times[500], times[900], times[990], time.Duration(sum/1000))
	}
	return nil
}
