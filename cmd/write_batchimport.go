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
	cmdWriteBatchImport = &cobra.Command{
		Use:   "batchimport",
		Short: "Write batchimport",
		RunE:  writeBatchImport,
	}

	wordList = []string{
		"Aldi Süd",
		"Aldi Nord",
		"Lidl",
		"Edeka",
		"Tengelmann",
    "Grosso",
		"allkauf",
		"neukauf",
		"Rewe",
		"Holdrio",
		"real",
		"Globus",
		"Norma",
		"Del Haize",
		"Spar",
		"Tesco",
		"Morrison",
}
)

type Point []float64

type Poly struct {
	Type          string `json:"type"`
	Coordinates   []Point `json:"coordinates"`
}

type Doc struct {
	Key           string `json:"_key"`
	Sha           string `json:"sha"`
	Payload       string `json:"payload"`
	Geo           *Poly  `Json:"geo,omitempty"`
	Words         string `json:"words,omitempty"`
}

// makeRandomPolygon makes a random GeoJson polygon.
func makeRandomPolygon() *Poly {
	ret := Poly{ Type: "polygon", Coordinates: make([]Point, 0, 5) }
	for i := 1; i <= 4 ; i += 1 {
		ret.Coordinates = append(ret.Coordinates,
			Point{ rand.Float64() * 300.0 - 90.0, rand.Float64() * 160.0 - 80.0})
	}
	return &ret
}

// makeRandomStringWithSpaces creates slice of bytes for the provided length.
// Each byte is in range from 33 to 123.
func makeRandomStringWithSpaces(length int) string {
	b := make([]byte, length, length)

	wordlen := rand.Int()%17 + 3
	for i := 0; i < length; i++ {
		wordlen -= 1
		if wordlen == 0 {
			wordlen = rand.Int()%17 + 3
			b[i] = byte(32)
	  } else {
			s := rand.Int()%52 + 65
			if s >= 91 {
				s += 6
		  }
			b[i] = byte(s)
		}
	}
	return string(b)
}

func makeRandomWords(nr int) string {
  b := make([]byte, 0, 15 * nr)
	for i := 1; i <= nr; i += 1 {
    if i > 1 {
			b = append(b, ' ')
		}
		b = append(b, []byte(wordList[rand.Int()%len(wordList)])...)
	}
	return string(b)
}

func init() {
	var parallelism int = 1
	var startDelay int64 = 5
	var number int64 = 1000000
	var payloadSize int64 = 10
	var batchSize int64 = 10000
	var collectionName string = "batchimport"
	var withGeo = true
	var withWords = 5
	cmdWriteBatchImport.Flags().IntVar(&parallelism, "parallelism", parallelism, "set -parallelism to use multiple go routines")
	cmdWriteBatchImport.Flags().Int64Var(&number, "number", number, "set -number for number of edges to write per go routine")
	cmdWriteBatchImport.Flags().Int64Var(&startDelay, "start-delay", startDelay, "Delay between the start of two go routines.")
	cmdWriteBatchImport.Flags().Int64Var(&payloadSize, "payload-size", payloadSize, "Size in bytes of payload in each document.")
	cmdWriteBatchImport.Flags().Int64Var(&batchSize, "batch-size", batchSize, "Size in number of documents of each import batch.")
	cmdWriteBatchImport.Flags().StringVar(&collectionName, "collection", collectionName, "Name of batch import collection.")
	cmdWriteBatchImport.Flags().BoolVar(&withGeo, "with-geo", withGeo, "Add some geo data to `geo` attribute.")
	cmdWriteBatchImport.Flags().IntVar(&withWords, "with-words", withWords, "Add so many words to `words` attribute.")
}

// writeBatchImport writes edges in parallel
func writeBatchImport(cmd *cobra.Command, _ []string) error {
	parallelism, _ := cmd.Flags().GetInt("parallelism")
	number, _ := cmd.Flags().GetInt64("number")
	startDelay, _ := cmd.Flags().GetInt64("start-delay")
	payloadSize, _ := cmd.Flags().GetInt64("payload-size")
	batchSize, _ := cmd.Flags().GetInt64("batch-size")
	collectionName, _ := cmd.Flags().GetString("collection")
	withGeo, _ := cmd.Flags().GetBool("with-geo")
	withWords, _ := cmd.Flags().GetInt("with-words")

	db, err := _client.Database(context.Background(), "_system")
	if err != nil {
		return errors.Wrapf(err, "can not get database: %s", "_system")
	}

	if err := writeSomeBatchesParallel(parallelism, number, startDelay, payloadSize, batchSize, collectionName, withGeo, withWords, db); err != nil {
		return errors.Wrapf(err, "can not do some batch imports")
	}

	return nil
}

// writeSomeBatchesParallel does some batch imports in parallel
func writeSomeBatchesParallel(parallelism int, number int64, startDelay int64, payloadSize int64, batchSize int64, collectionName string, withGeo bool, withWords int, db driver.Database) error {
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
			err := writeSomeBatches(number, int64(i), payloadSize, batchSize, collectionName, withGeo, withWords, db, &mutex)
			if err != nil {
				fmt.Printf("writeSomeBatches error: %v\n", err)
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
	batchesPerSec := float64(int64(parallelism) * number) / (float64(totaltime) / float64(time.Second))
	docspersec := float64(int64(parallelism) * number * batchSize) / (float64(totaltime) / float64(time.Second))
	fmt.Printf("\nTotal number of documents written: %d, total time: %v, total batches per second: %f, total docs per second: %f\n", int64(parallelism) * number * batchSize, totaltimeend.Sub(totaltimestart), batchesPerSec, docspersec)
	if !haveError {
		return nil
	}
	fmt.Printf("Error in writeSomeBatches.\n")
	return fmt.Errorf("Error in writeSomeBatches.")
}

// writeSomeBatches writes `nrBatches` batches with `batchSize` documents.
func writeSomeBatches(nrBatches int64, id int64, payloadSize int64, batchSize int64, collectionName string, withGeo bool, withWords int, db driver.Database, mutex *sync.Mutex) error {
	edges, err := db.Collection(nil, collectionName)
	if err != nil {
		fmt.Printf("writeSomeBatches: could not open `%s` collection: %v\n", collectionName, err)
		return err
	}
	docs := make([]Doc, 0, batchSize)
  times := make([]time.Duration, 0, batchSize)
	cyclestart := time.Now()
	last100start := cyclestart
	for i := int64(1); i <= nrBatches; i++ {
		start := time.Now()
    for j := int64(1); j <= batchSize; j++ {
			x := fmt.Sprintf("%d", (id * nrBatches + i - 1) * batchSize + j)
			key := fmt.Sprintf("%x", sha256.Sum256([]byte(x)))
			x = "SHA" + x
			sha := fmt.Sprintf("%x", sha256.Sum256([]byte(x)))
			pay := makeRandomStringWithSpaces(int(payloadSize))
			var poly *Poly
			if withGeo {
        poly = makeRandomPolygon()
			}
			var words string
			if withWords > 0 {
				words = makeRandomWords(withWords)
		  }
			docs = append(docs, Doc{
				Key: key, Sha: sha, Payload: pay, Geo: poly, Words: words })
	  }
		ctx, cancel := context.WithTimeout(driver.WithOverwriteMode(context.Background(), driver.OverwriteModeIgnore), time.Hour)
		_, _, err := edges.CreateDocuments(ctx, docs)
		cancel()
		if err != nil {
			fmt.Printf("writeSomeBatches: could not write batch: %v\n", err)
			return err
		}
		docs = docs[0:0]
    times = append(times, time.Now().Sub(start))
		if i % 100 == 0 {
			dur := float64(time.Now().Sub(last100start)) / float64(time.Second)
			mutex.Lock()
			fmt.Printf("%s Have imported %d batches for id %d, last 100 took %f seconds.\n", time.Now(), int(i), id, dur)
			mutex.Unlock()
		}
	}
	sort.Sort(DurationSlice(times))
	var sum int64 = 0
	for _, t := range times {
		sum = sum + int64(t)
	}
	totaltime := time.Now().Sub(cyclestart)
	nrDocs := batchSize * nrBatches
	docspersec := float64(nrDocs) / (float64(totaltime) / float64(time.Second))
	mutex.Lock()
	fmt.Printf("Times for %d batches (per batch): %s (median), %s (90%%ile), %s (99%%ilie), %s (average), docs per second in this go routine: %f\n", nrBatches, times[int(float64(0.5) * float64(nrBatches))], times[int(float64(0.9) * float64(nrBatches))], times[int(float64(0.99) * float64(nrBatches))], time.Duration(sum / nrBatches), docspersec)
	mutex.Unlock()
	return nil
}
