package cmd

import (
	"context"
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"time"
)

var (
	cmdElCheapoWrites = &cobra.Command{
		Use:   "elcheapo",
		Short: "Write edges transactional",
		RunE:  writeEdgesElCheapo,
	}
)

func init() {
	var parallelism int = 1
	var number int64 = 1000000
	cmdElCheapoWrites.Flags().IntVar(&parallelism, "parallelism", parallelism, "set -parallelism to use multiple go routines")
	cmdElCheapoWrites.Flags().Int64Var(&number, "number", number, "set -number for number of edges to write per go routine")
}

// writeEdges writes edges in parallel
func writeEdgesElCheapo(cmd *cobra.Command, _ []string) error {
	parallelism, _ := cmd.Flags().GetInt("parallelism")
	number, _ := cmd.Flags().GetInt64("number")

	db, err := _client.Database(context.Background(), "_system")
	if err != nil {
		return errors.Wrapf(err, "can not get database: %s", "_system")
	}

	if err := writeSomeEdgesParallelElCheapo(parallelism, number, db); err != nil {
		return errors.Wrapf(err, "can not setup some tenants")
	}

	return nil
}

// writeSomeEdges creates some edges in parallel
func writeSomeEdgesParallelElCheapo(parallelism int, number int64, db driver.Database) error {
	var mutex sync.Mutex
	totaltimestart := time.Now()
	wg := sync.WaitGroup{}
	haveError := false
	for i := 1; i <= parallelism; i++ {
	  time.Sleep(5 * time.Millisecond)
		i := i // bring into scope
		wg.Add(1)

		go func(wg *sync.WaitGroup, i int) {
			defer wg.Done()
			fmt.Printf("Starting go routine...\n")
			id := "id_" + strconv.FormatInt(int64(i), 10)
			err := writeSomeEdgesElCheapo(number, id, db, &mutex)
			if err != nil {
				fmt.Printf("writeSomeEdgesElCheapo error: %v\n", err)
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
	fmt.Printf("Error in writeSomeEdges.\n")
	return fmt.Errorf("Error in writeSomeEdges.")
}

// writeOneTenant writes `nrPaths` short paths into the smart graph for
// tenant with id `tenantId`.
func writeSomeEdgesElCheapo(nrEdges int64, id string, db driver.Database, mutex *sync.Mutex) error {
	edges, err := db.Collection(nil, "edges")
	if err != nil {
		fmt.Printf("writeSomeEdgesElCheapo: could not open `edges` collection: %v\n", err)
		return err
	}
	eds := make([]Edge, 0, 1000)
  times := make([]time.Duration, 0, 1000)
	cyclestart := time.Now()
	tcolls := driver.TransactionCollections{
		Write: []string{"edges"},
	}
	topts := driver.BeginTransactionOptions{
	}
	for i := int64(1); i <= nrEdges / 1000; i++ {
		start := time.Now()
    for j := 1; j <= 1000; j++ {
			fromUid := rand.Intn(10000)
			toUid := rand.Intn(10000)
			eds = append(eds, Edge{
					From: "pubmed/U" + strconv.FormatInt(int64(fromUid), 10),
					To: "pubmed/U" + strconv.FormatInt(int64(toUid), 10),
					FromUid: fromUid,
					ToUid: toUid,
          Score: rand.Intn(10000000),
					Last_modified: time.Now().Format(time.RFC3339),
			})
	  }
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		tid, err := db.BeginTransaction(ctx, tcolls, &topts)
		if err != nil {
			cancel()
			fmt.Printf("writeSomeEdgesElCheapo: could not begin transaction: %v\n", err)
			return err
		}
    ctx2 := driver.WithTransactionID(ctx, tid)
		_, _, err = edges.CreateDocuments(ctx2, eds)
		if err != nil {
			_ = db.AbortTransaction(ctx, tid, &driver.AbortTransactionOptions{})
			cancel()
			fmt.Printf("writeSomeEdgesElCheapo: could not write edges: %v\n", err)
			return err
		}
		err = db.CommitTransaction(ctx, tid, &driver.CommitTransactionOptions{})
		cancel()
		if err != nil {
			fmt.Printf("writeSomeEdgesElCheap: could not commit transaction: %v\n", err)
			return err
		}

		cancel()
		eds = eds[0:0]
		if i % 100 == 0 {
			mutex.Lock()
			fmt.Printf("%s Have imported %d paths for id %s.\n", time.Now(), i * 1000, id)
			mutex.Unlock()
			cancel()
		}
    times = append(times, time.Now().Sub(start))
		if len(times) % 100 == 0 {
			sort.Sort(DurationSlice(times))
			var sum int64 = 0
			for _, t := range times {
				sum = sum + int64(t)
			}
			totaltime := time.Now().Sub(cyclestart)
			docspersec := 100000.0 / (float64(totaltime) / float64(time.Second))
			mutex.Lock()
			fmt.Printf("Times for last 100 writes (=100000 edges): %s (median), %s (90%%ile), %s (99%%ilie), %s (average), edges per second in this go routine: %f\n", times[50], times[90], times[99], time.Duration(sum/100), docspersec)
			mutex.Unlock()
			times = times[0:0]
			cyclestart = time.Now()
		}
	}
	return nil
}
