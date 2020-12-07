package cmd

import (
	"context"
	"fmt"
	"github.com/arangodb/go-driver"
  "github.com/neunhoef/collectionmaker/pkg/database"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"time"
)

var (
	cmdWriteGraph = &cobra.Command{
		Use:   "graph",
		Short: "Write vertices and edges and update them",
		RunE:  writeGraph,
	}
)

// This uses the types Instance and Step from "create_graph.go"

func init() {
	var parallelism int = 1
	var startDelay int64 = 5
	var number int64 = 1000000
	cmdWriteGraph.Flags().IntVar(&parallelism, "parallelism", parallelism, "set -parallelism to use multiple go routines")
	cmdWriteGraph.Flags().Int64Var(&number, "number", number, "set -number for number of edges to write per go routine")
	cmdWriteGraph.Flags().Int64Var(&startDelay, "start-delay", startDelay, "Delay between the start of two go routines.")
}

// writeGraph writes edges in parallel
func writeGraph(cmd *cobra.Command, _ []string) error {
	parallelism, _ := cmd.Flags().GetInt("parallelism")
	number, _ := cmd.Flags().GetInt64("number")
	startDelay, _ := cmd.Flags().GetInt64("start-delay")

	db, err := _client.Database(context.Background(), "_system")
	if err != nil {
		return errors.Wrapf(err, "can not get database: %s", "_system")
	}

	if err := writeGraphParallel(parallelism, number, startDelay, db); err != nil {
		return errors.Wrapf(err, "can not setup some tenants")
	}

	return nil
}

// writeSomeGraphParallel creates some edges in parallel
func writeGraphParallel(parallelism int, number int64, startDelay int64, db driver.Database) error {
	var mutex sync.Mutex
	totaltimestart := time.Now()
	wg := sync.WaitGroup{}
	haveError := false
	for i := 1; i <= parallelism; i++ {
	  time.Sleep(time.Duration(startDelay) * time.Millisecond)
		i := i // bring into scope
		wg.Add(1)

		go func(wg *sync.WaitGroup, i int) {
			defer wg.Done()
			fmt.Printf("Starting go routine...\n")
			id := "id_" + strconv.FormatInt(int64(i), 10)
			err := writeSomeGraph(number, id, db, &mutex)
			if err != nil {
				fmt.Printf("writeSomeGraph error: %v\n", err)
				haveError = true
			}
			mutex.Lock()
			fmt.Printf("Go routine %d done\n", i)
			mutex.Unlock()
		}(&wg, i)
	}

	wg.Wait()
	totaltimeend := time.Now()
	totaltime := totaltimeend.Sub(totaltimestart)
	docspersec := float64(int64(parallelism) * number) / (float64(totaltime) / float64(time.Second))
	fmt.Printf("\nTotal number of edges written: %d, total time: %v, total edges per second: %f\n", int64(parallelism) * number, totaltimeend.Sub(totaltimestart), docspersec)
	if !haveError {
		return nil
	}
	fmt.Printf("Error in writeSomeGraph.\n")
	return fmt.Errorf("Error in writeSomeGraph.")
}

// writeOneTenant does `nr` write operations, alternating between vertices
// and edges and inserts and updates.
func writeSomeGraph(nr int64, id string, db driver.Database, mutex *sync.Mutex) error {
	instances, err := db.Collection(nil, "instances")
	if err != nil {
		fmt.Printf("writeSomeGraph: could not open `instances` collection: %v\n", err)
		return err
	}
	steps, err := db.Collection(nil, "steps")
	if err != nil {
		fmt.Printf("writeSomeGraph: could not open `steps` collection: %v\n", err)
		return err
	}
	optype := 0   // changes from 0 to 3 and then back to 0
  times := make([]time.Duration, 0, 10000)
	cyclestart := time.Now()
	randomLargeString := database.MakeRandomString(1400)
	randomSmallString := database.MakeRandomString(700)
	tenant := int64(1)
	previous := int64(0)
	for i := int64(0); i < nr; i++ {
		start := time.Now()
		switch (optype) {
		case 0:  // write a new vertex
		  inst := Instance{
				Key: "I" + id + "_" + strconv.FormatInt(i/4, 10),
			  TenantId: "T" + strconv.FormatInt(tenant, 10),
				Payload: strconv.FormatInt(i, 10) + randomLargeString,
		  }
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			_, err := instances.CreateDocument(ctx, &inst)
			cancel()
			if err != nil {
				fmt.Printf("writeSomeGraph: could not write vertex: %v\n", err)
				return err
			}
		case 1:  // write a new edge
		  step := Step{
				Key: "S" + id + "_" + strconv.FormatInt(i/4, 10),
				From: "instances/I" + id + "_" + strconv.FormatInt(i/4, 10),
				To:   "instances/I" + id + "_" + strconv.FormatInt(i/4, 10),
			  TenantId: "T" + strconv.FormatInt(tenant, 10),
				Payload: strconv.FormatInt(i, 10) + randomSmallString,
		  }
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			_, err := steps.CreateDocument(ctx, &step)
			cancel()
			if err != nil {
				fmt.Printf("writeSomeGraph: could not write edge: %v\n", err)
				return err
			}
		case 2:  // modify an existing vertex
			key := "I" + id + "_" + strconv.FormatInt(previous, 10)
		  inst := Instance{
				Key: key,
			  TenantId: "T" + strconv.FormatInt(tenant, 10),
				Payload: strconv.FormatInt(i, 10) + randomLargeString,
		  }
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			_, err := instances.ReplaceDocument(ctx, key, &inst)
			cancel()
			if err != nil {
				fmt.Printf("writeSomeGraph: could not replace vertex: %v\n", err)
				return err
			}
		case 3:  // modify an existing edge
			key := "S" + id + "_" + strconv.FormatInt(previous, 10)
		  step := Step{
				Key: key,
				From: "instances/I" + id + "_" + strconv.FormatInt(i/4, 10),
				To:   "instances/I" + id + "_" + strconv.FormatInt(i/4, 10),
			  TenantId: "T" + strconv.FormatInt(tenant, 10),
				Payload: strconv.FormatInt(i, 10) + randomLargeString,
		  }
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			_, err := steps.ReplaceDocument(ctx, key, &step)
			cancel()
			if err != nil {
				fmt.Printf("writeSomeGraph: could not replace edge: %v\n", err)
				return err
			}
	  }

    times = append(times, time.Now().Sub(start))
		if ((i+1) % 10000 == 0 || i == nr - 1) && len(times) != 0 {
			mutex.Lock()
			fmt.Printf("%s Have imported %d paths for id %s.\n", time.Now(), i+1, id)
			mutex.Unlock()
			sort.Sort(DurationSlice(times))
			number := int64(len(times))
			var sum int64 = 0
			for _, t := range times {
				sum = sum + int64(t)
			}
			totaltime := time.Now().Sub(cyclestart)
			docspersec := float64(number) / (float64(totaltime) / float64(time.Second))
			mutex.Lock()
			fmt.Printf("Times for last %d writes: %s (median), %s (90%%ile), %s (99%%ilie), %s (average), edges per second in this go routine: %f\n", number, times[number / 2], times[number * 9 / 10], times[number * 99 / 100], time.Duration(sum/number), docspersec)
			mutex.Unlock()
			times = times[0:0]
			cyclestart = time.Now()
		}
		optype = (optype + 1) & 3
		tenant = (tenant + 1) & 65535
		if i < 4 {
			previous = 0
		} else if i < 200 {
			previous = rand.Int63n(i/4)
	  } else {
			previous = previous + 47
			limit := i/4-1
			for previous >= limit {
				previous = previous - limit
			}
		}
	}
	return nil
}
