package cmd

import (
	"context"
	"crypto/sha256"
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"math/rand"
	"sort"
	"sync"
	"time"
)

var (
	cmdReadBatchImport = &cobra.Command{
		Use:   "batchimport",
		Short: "Read batchimport",
		RunE:  readBatchImport,
	}

)

func init() {
	var parallelism int = 1
	var startDelay int64 = 5
	var number int64 = 1000000
	var totalNumber int64 = 1000000
	var collectionName string = "batchimport"
	var readFromFollower bool = false
	cmdReadBatchImport.Flags().IntVar(&parallelism, "parallelism", parallelism, "set -parallelism to use multiple go routines")
	cmdReadBatchImport.Flags().Int64Var(&number, "number", number, "set -number for number of documents to read per go routine")
	cmdReadBatchImport.Flags().Int64Var(&totalNumber, "total-number", totalNumber, "set -total-number for the total number of documents in the collection")
	cmdReadBatchImport.Flags().Int64Var(&startDelay, "start-delay", startDelay, "Delay between the start of two go routines.")
	cmdReadBatchImport.Flags().StringVar(&collectionName, "collection", collectionName, "Name of batch import collection.")
	cmdReadBatchImport.Flags().BoolVar(&readFromFollower, "read-from-follower", readFromFollower, "Use read-from-followers (aka allow-dirty-reads).")
}

// readBatchImport reads docs in parallel
func readBatchImport(cmd *cobra.Command, _ []string) error {
	parallelism, _ := cmd.Flags().GetInt("parallelism")
	number, _ := cmd.Flags().GetInt64("number")
	totalNumber, _ := cmd.Flags().GetInt64("total-number")
	startDelay, _ := cmd.Flags().GetInt64("start-delay")
	collectionName, _ := cmd.Flags().GetString("collection")
	readFromFollower, _ := cmd.Flags().GetBool("read-from-follower")

	db, err := _client.Database(context.Background(), "_system")
	if err != nil {
		return errors.Wrapf(err, "can not get database: %s", "_system")
	}

	if err := readSomeParallel(parallelism, number, startDelay, totalNumber, collectionName, readFromFollower, db); err != nil {
		return errors.Wrapf(err, "can not do some batchimport reads")
	}

	return nil
}

// readSomeBatchesParallel does some batch imports in parallel
func readSomeParallel(parallelism int, number int64, startDelay int64, totalNumber int64, collectionName string, readFromFollower bool, db driver.Database) error {
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
			err := readSome(number, int64(i), totalNumber, collectionName, readFromFollower, db, &mutex)
			if err != nil {
				fmt.Printf("readSome error: %v\n", err)
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
	docsPerSec := float64(int64(parallelism) * number) / (float64(totaltime) / float64(time.Second))
	fmt.Printf("\nTotal number of documents read: %d, total time: %v, total docs per second: %f\n", int64(parallelism) * number , totaltimeend.Sub(totaltimestart), docsPerSec)
	if !haveError {
		return nil
	}
	fmt.Printf("Error in readSome.\n")
	return fmt.Errorf("Error in readSome.")
}

// readSomeBatches reads `nrDocs` documents (random reads).
func readSome(nrDocs int64, id int64, totalNumber int64, collectionName string, readFromFollower bool, db driver.Database, mutex *sync.Mutex) error {
	docs, err := db.Collection(nil, collectionName)
	if err != nil {
		fmt.Printf("readSome: could not open `%s` collection: %v\n", collectionName, err)
		return err
	}
	var doc Doc
  times := make([]time.Duration, 0, nrDocs)
	cyclestart := time.Now()
	last100start := cyclestart
	source := rand.New(rand.NewSource(int64(id) + rand.Int63()))
	for i := int64(1); i <= nrDocs; i++ {
		start := time.Now()
		which := source.Int63n(totalNumber)
		x := fmt.Sprintf("%d", which)
		key := fmt.Sprintf("%x", sha256.Sum256([]byte(x)))
		var allowDirtyReads bool = readFromFollower
		ctx, cancel := context.WithTimeout(driver.WithAllowDirtyReads(context.Background(), &allowDirtyReads), time.Hour)

		_, err := docs.ReadDocument(ctx, key, &doc)
		cancel()
		if err != nil {
			fmt.Printf("readSome: could not read document: %v\n", err)
			return err
		}
    times = append(times, time.Now().Sub(start))
		if i % 100000 == 0 {
			dur := float64(time.Now().Sub(last100start)) / float64(time.Second)
			mutex.Lock()
			fmt.Printf("%s Have read %d docs for id %d, last 100000 took %f seconds.\n", time.Now(), int(i), id, dur)
			mutex.Unlock()
		}
	}
	sort.Sort(DurationSlice(times))
	var sum int64 = 0
	for _, t := range times {
		sum = sum + int64(t)
	}
	totaltime := time.Now().Sub(cyclestart)
	docspersec := float64(nrDocs) / (float64(totaltime) / float64(time.Second))
	mutex.Lock()
	fmt.Printf("Times for reading %d docs: %s (median), %s (90%%ile), %s (99%%ilie), %s (average), docs per second in this go routine: %f\n", nrDocs, times[int(float64(0.5) * float64(nrDocs))], times[int(float64(0.9) * float64(nrDocs))], times[int(float64(0.99) * float64(nrDocs))], time.Duration(sum / nrDocs), docspersec)
	mutex.Unlock()
	return nil
}
