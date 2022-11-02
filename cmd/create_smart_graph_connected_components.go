package cmd

import (
	"context"
	"fmt"
	"math/bits"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/neunhoef/collectionmaker/pkg/database"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/arangodb/go-driver"
)

const (
	defaultNumberOfParts        = 12
	defaultVertexPayloadLength  = 16
	defaultEdgePayloadLength    = 16
	defaultLog2NumberOfVertices = 20
)

var (
	cmdCreateSmartGraphConnectedComponents = &cobra.Command{
		Use:   "smartgraph",
		Short: "Create smart graph for connected components",
		RunE:  createSmartGraph,
	}
)

type Vertex struct {
	Key       string `json:"_key"`
	SmartPart string `json:"smartPart"`
	Payload   string `json:"payload"`
}

type Link struct {
	Key     string `json:"_key,omitempty"`
	From    string `json:"_from"`
	To      string `json:"_to"`
	Payload string `json:"payload"`
}

func smartGraphFlags(command *cobra.Command) {
	var numberOfParts, vertexPayloadLength, edgePayloadLength int
	var log2NumberOfVertices int
	var parallelism int
	var numberOfShards int

	command.Flags().IntVar(&numberOfParts, "numberOfParts", defaultNumberOfParts, "Number of parts of graph to create")
	command.Flags().IntVar(&vertexPayloadLength, "vertexPayloadLength", defaultVertexPayloadLength, "Size in bytes of payload for vertices")
	command.Flags().IntVar(&edgePayloadLength, "edgePayloadLength", defaultEdgePayloadLength, "Size in bytes of payload for edges")
	command.Flags().IntVar(&log2NumberOfVertices, "log2NumberOfVertices", defaultLog2NumberOfVertices, "Log 2 of number of vertices in each part")
	command.Flags().IntVar(&parallelism, "parallelism", 1, "Number of go routines")
	command.Flags().IntVar(&numberOfShards, "shards", 3, "Number of shards")
}

func init() {
	var drop = false

	cmdCreateSmartGraphConnectedComponents.Flags().BoolVar(&drop, "drop", drop, "set -drop to true to drop data before start")
	smartGraphFlags(cmdCreateSmartGraphConnectedComponents)
}

func createSmartGraph(cmd *cobra.Command, _ []string) error {
	drop, _ := cmd.Flags().GetBool("drop")
	numberOfParts, _ := cmd.Flags().GetInt("numberOfParts")
	vertexPayloadLength, _ := cmd.Flags().GetInt("vertexPayloadLength")
	edgePayloadLength, _ := cmd.Flags().GetInt("edgePayloadLength")
	log2NumberOfVertices, _ := cmd.Flags().GetInt("log2NumberOfVertices")
	parallelism, _ := cmd.Flags().GetInt("parallelism")
	numberOfShards, _ := cmd.Flags().GetInt("shards")

	db, err := _client.Database(context.Background(), "_system")
	if err != nil {
		return errors.Wrapf(err, "can not get database: %s", "_system")
	}

	if setupSmart(drop, db, numberOfShards) != nil {
		return errors.Wrapf(err, "setup was already launched")
	}

	if err := setupSomeParts(numberOfParts, vertexPayloadLength, edgePayloadLength, log2NumberOfVertices, parallelism, db); err != nil {
		return errors.Wrapf(err, "can not setup some parts")
	}

	return nil
}

// setupSomeParts creates some parts in parallel
func setupSomeParts(numberOfParts int, vertexPayloadLength int,
	edgePayloadLength int, log2NumberOfVertices int, parallelism int,
	db driver.Database) error {
	wg := sync.WaitGroup{}
	haveError := false
	throttle := make(chan int, parallelism)
	for i := 1; i <= numberOfParts; i++ {
		i := i // bring into scope
		wg.Add(1)

		go func(wg *sync.WaitGroup, i int) {
			defer wg.Done()
			throttle <- i
			fmt.Printf("Starting go routine...\n")
			partId := strconv.FormatInt(int64(i), 10)
			err := writeOnePart(partId, vertexPayloadLength, edgePayloadLength, log2NumberOfVertices, db)
			if err != nil {
				fmt.Printf("setupSomeParts error: %v\n", err)
				haveError = true
			}
			fmt.Printf("Go routine %d done", <-throttle)
		}(&wg, i)
	}

	wg.Wait()
	if !haveError {
		return nil
	}
	fmt.Printf("Error in setupSomeParts.\n")
	return fmt.Errorf("Error in setupSomeParts.")
}

// writeOnePart writes one part into the smart graph for id `partId`.
func writeOnePart(partId string, vertexPayloadLength int, edgePayloadLength int, log2NumberOfVertices int, db driver.Database) error {
	vertices, err := db.Collection(nil, "vertices")
	if err != nil {
		fmt.Printf("writeOnePart: could not open `vertices` collection: %v\n", err)
		return err
	}
	links, err := db.Collection(nil, "links")
	if err != nil {
		fmt.Printf("writeOnePart: could not open `links` collection: %v\n", err)
		return err
	}
	ver := make([]Vertex, 0, 3000)
	var nr int64 = 1
	for j := 1; j <= log2NumberOfVertices; j++ {
		nr *= 2
	}
	var i int64 = 0
	for i = 1; i <= nr; i++ {
		n := strconv.FormatInt(int64(i), 10)
		v := Vertex{
			Key:       partId + ":K" + n,
			SmartPart: partId,
			Payload:   database.MakeRandomString(vertexPayloadLength),
		}
		ver = append(ver, v)
		if len(ver) >= 3000 || i == nr {
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			_, _, err := vertices.CreateDocuments(ctx, ver)
			if err != nil {
				fmt.Printf("writeOnePart: could not write vertices: %v\n", err)
				cancel()
				return err
			}
			ver = ver[0:0]
			fmt.Printf("%s Have imported %d vertices for part %s.\n", time.Now(), i, partId)
			cancel()
		}
	}
	// Now create two edges for each vertex:
	lin := make([]Link, 0, 3000)
	for i = 1; i <= nr; i++ {
		comp := bits.TrailingZeros64(uint64(i))
		tmp := uint64(1) << comp
		j := ((rand.Uint64()%uint64(nr) + 1) &^ (tmp - 1)) | tmp
		li1 := Link{
			From:    "vertices/" + partId + ":K" + strconv.FormatInt(i, 10),
			To:      "vertices/" + partId + ":K" + strconv.FormatInt(int64(j), 10),
			Payload: database.MakeRandomString(edgePayloadLength),
		}
		li1b := Link{
			From:    "vertices/" + partId + ":K" + strconv.FormatInt(int64(j), 10),
			To:      "vertices/" + partId + ":K" + strconv.FormatInt(i, 10),
			Payload: database.MakeRandomString(edgePayloadLength),
		}
		j = ((rand.Uint64()%uint64(nr) + 1) &^ (tmp - 1)) | tmp
		li2 := Link{
			From:    "vertices/" + partId + ":K" + strconv.FormatInt(i, 10),
			To:      "vertices/" + partId + ":K" + strconv.FormatInt(int64(j), 10),
			Payload: database.MakeRandomString(edgePayloadLength),
		}
		li2b := Link{
			From:    "vertices/" + partId + ":K" + strconv.FormatInt(int64(j), 10),
			To:      "vertices/" + partId + ":K" + strconv.FormatInt(i, 10),
			Payload: database.MakeRandomString(edgePayloadLength),
		}
		lin = append(lin, li1, li1b, li2, li2b)
		if len(lin) >= 3000 || i == nr {
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			_, _, err = links.CreateDocuments(ctx, lin)
			if err != nil {
				fmt.Printf("writeOnePart: could not write links: %v\n", err)
				cancel()
				return err
			}
			lin = lin[0:0]
			fmt.Printf("%s Have imported %d links for part %s.\n", time.Now(), 2*i, partId)
			cancel()
		}
	}
	return nil
}

// setup will set up a disjoint smart graph, if the smart graph is already
// there, it will not complain.
func setupSmart(drop bool, db driver.Database, numberOfShards int) error {
	sg, err := db.Graph(nil, "SmartGraph")
	if err == nil {
		if !drop {
			fmt.Printf("Found smart graph already, setup is already done.\n")
			return nil
		}
		err = sg.Remove(nil)
		if err != nil {
			fmt.Printf("Could not drop smart graph: %v\n", err)
			return err
		}
		sts, err := db.Collection(nil, "links")
		if err != nil {
			fmt.Printf("Did not find `links` collection: %v\n", err)
			return err
		}
		if err = sts.Remove(nil); err != nil {
			fmt.Printf("Could not drop edges: %v\n", err)
			return err
		}
		ins, err := db.Collection(nil, "vertices")
		if err != nil {
			fmt.Printf("Did not find `vertices` collection: %v\n", err)
			return err
		}
		if err = ins.Remove(nil); err != nil {
			fmt.Printf("Could not drop vertices: %v\n", err)
			return err
		}
	} else if !driver.IsNotFound(err) {
		fmt.Printf("Error: could not look for smart graph: %v\n", err)
		return err
	}

	// Now create the graph:
	_, err = db.CreateGraph(nil, "SmartGraph", &driver.CreateGraphOptions{
		EdgeDefinitions: []driver.EdgeDefinition{{
			Collection: "links",
			From:       []string{"vertices"},
			To:         []string{"vertices"},
		}},
		IsSmart:             true,
		SmartGraphAttribute: "smartPart",
		NumberOfShards:      numberOfShards,
		ReplicationFactor:   3,
		IsDisjoint:          true,
	})
	if err != nil {
		fmt.Printf("Error: could not create smart graph: %v\n", err)
		return err
	}
	return nil
}
