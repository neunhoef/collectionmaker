package main

import (
	"context"
	"flag"
	"fmt"
	driver "github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// setup will set up a disjoint smart graph, if the smart graph is already
// there, it will not complain.
func setup(drop bool, db driver.Database) error {
	sg, err := db.Graph(nil, "G")
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
		sts, err := db.Collection(nil, "steps")
		if err != nil {
			fmt.Printf("Did not find `steps` collection: %v\n", err)
			return err
		}
		if err = sts.Remove(nil); err != nil {
			fmt.Printf("Could not drop edges: %v\n", err)
			return err
		}
		ins, err := db.Collection(nil, "instances")
		if err != nil {
			fmt.Printf("Did not find `instances` collection: %v\n", err)
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
	_, err = db.CreateGraph(nil, "G", &driver.CreateGraphOptions{
		EdgeDefinitions: []driver.EdgeDefinition{{
			Collection: "steps",
			From:       []string{"instances"},
			To:         []string{"instances"},
		}},
		IsSmart:             true,
		SmartGraphAttribute: "tenantId",
		NumberOfShards:      3,
		ReplicationFactor:   3,
		IsDisjoint:          true,
	})
	if err != nil {
		fmt.Printf("Error: could not create smart graph: %v\n", err)
		return err
	}
	return nil
}

type Instance struct {
	Key      string `json:"_key"`
	TenantId string `json:"tenantId"`
	Payload  string `json:"payload"` // approx length 1400 bytes
}

type Step struct {
	TenantId string `json:"tenantId"`
	From     string `json:"_from"`
	To       string `json:"_to"`
	Payload  string `json:"payload"` // approx length 700 bytes
}

func makeRandomString(length int) string {
	b := make([]byte, length, length)
	for i := 0; i < length; i++ {
		s := rand.Int()%90 + 33
		b[i] = byte(s)
	}
	return string(b)
}

// writeOneTenant writes `nrPaths` short paths into the smart graph for
// tenant with id `tenantId`.
func writeOneTenant(nrPaths int, tenantId string, db driver.Database) error {
	instances, err := db.Collection(nil, "instances")
	if err != nil {
		fmt.Printf("writeOneTenant: could not open `instances` collection: %v\n", err)
		return err
	}
	steps, err := db.Collection(nil, "steps")
	if err != nil {
		fmt.Printf("writeOneTenant: could not open `steps` collection: %v\n", err)
		return err
	}
	ins := make([]Instance, 0, 3000)
	sts := make([]Step, 0, 2000)
	for i := 1; i <= nrPaths; i++ {
		n := strconv.FormatInt(int64(i), 10)
		in1 := Instance{
			Key:      tenantId + ":K" + n,
			TenantId: tenantId,
			Payload:  makeRandomString(1400),
		}
		in2 := Instance{
			Key:      tenantId + ":L" + n,
			TenantId: tenantId,
			Payload:  makeRandomString(1400),
		}
		in3 := Instance{
			Key:      tenantId + ":M" + n,
			TenantId: tenantId,
			Payload:  makeRandomString(1400),
		}
		st1 := Step{
			TenantId: tenantId,
			From:     "instances/" + tenantId + ":K" + n,
			To:       "instances/" + tenantId + ":L" + n,
			Payload:  makeRandomString(700),
		}
		st2 := Step{
			TenantId: tenantId,
			From:     "instances/" + tenantId + ":L" + n,
			To:       "instances/" + tenantId + ":M" + n,
			Payload:  makeRandomString(700),
		}
		ins = append(ins, in1, in2, in3)
		sts = append(sts, st1, st2)
		if len(ins) >= 3000 || i == nrPaths {
      ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			_, _, err := instances.CreateDocuments(ctx, ins)
			if err != nil {
				fmt.Printf("writeOneTenant: could not write instances: %v\n", err)
				cancel()
				return err
			}
			_, _, err = steps.CreateDocuments(ctx, sts)
			if err != nil {
				fmt.Printf("writeOneTenant: could not write steps: %v\n", err)
				cancel()
				return err
			}
			ins = ins[0:0]
			sts = sts[0:0]
			fmt.Printf("%s Have imported %d paths for tenant %s.\n", time.Now(), i, tenantId)
			cancel()
		}

	}
	return nil
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

// setupSomeTenants creates some tenants in parallel
func setupSomeTenants(firstTenantNr, lastTenantNr int,
	nrPathsPerTenant int, parallelism int, db driver.Database) error {
	wg := sync.WaitGroup{}
	haveError := false
	throttle := make(chan int, parallelism)
	for i := firstTenantNr; i <= lastTenantNr; i++ {
		i := i // bring into scope
		wg.Add(1)

		go func(wg *sync.WaitGroup, i int) {
			defer wg.Done()
			throttle <- i
			fmt.Printf("Starting go routine...\n")
			tenantId := "ten" + strconv.FormatInt(int64(i), 10)
			err := writeOneTenant(nrPathsPerTenant, tenantId, db)
			if err != nil {
				fmt.Printf("setupSomeTenants error: %v\n", err)
				haveError = true
			}
			fmt.Printf("Go routine %d done", <-throttle)
		}(&wg, i)
	}

	wg.Wait()
	if !haveError {
		return nil
	}
	fmt.Printf("Error in setupSomeTenants.\n")
	return fmt.Errorf("Error in setupSomeTenants.")
}

func runRandomTest(db driver.Database, runTimeSeconds int,
	firstTenantNr int, lastTenantNr int,
	pathsPerTenant int, parallelism int) error {
	// parallelism ignored so far!
	startTime := time.Now()
	limit := time.Duration(runTimeSeconds) * time.Second
	for time.Now().Sub(startTime) < limit {
		// Run another 10000 random access queries:
		times := make([]time.Duration, 0, 10000)
		for i := 1; i <= 10000; i++ {
			start := time.Now()
			startVertex := fmt.Sprintf(
				"instances/ten%d:K%d",
				firstTenantNr+rand.Intn(lastTenantNr+1-firstTenantNr),
				rand.Intn(pathsPerTenant))
			query := fmt.Sprintf(
				`FOR v, e IN 2..2 OUTBOUND "%s" GRAPH "G" RETURN v`,
				startVertex)
			fmt.Printf("Query: %s\n", query)
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
				fmt.Printf("Got wrong count: %d\n", cursor.Count())
				return fmt.Errorf("Banana")
			}
		}
		sort.Sort(DurationSlice(times))
		var sum int64 = 0
		for _, t := range times {
			sum = sum + int64(t)
		}
		fmt.Printf("Times for last 10000: %s (median), %s (90%%ile), %s (99%%ilie), %s (average)", times[5000], times[9000], times[9900], time.Duration(sum/10000))
	}
	return nil
}

const (
	ModeCreate string = "create"
	ModeTest   string = "test"
)

var (
	drop             bool   = false
	firstTenant      int    = 1
	lastTenant       int    = 3000
	nrPathsPerTenant int    = 10000
	parallelism      int    = 4
	endpoint         string = "http://localhost:8529"
	mode             string = "create"
	runTimeSeconds   int    = 30
)

func main() {
	fmt.Print("Hello world!\n")

	flag.BoolVar(&drop, "drop", drop, "set -drop to true to drop data before start")
	flag.IntVar(&firstTenant, "firstTenant", firstTenant, "Index of first tenant to create")
	flag.IntVar(&lastTenant, "lastTenant", lastTenant, "Index of last tenant to create")
	flag.IntVar(&nrPathsPerTenant, "nrPathsPerTenant", nrPathsPerTenant, "Number of paths per tenant")
	flag.IntVar(&parallelism, "parallelism", parallelism, "Parallelism")
	flag.StringVar(&endpoint, "endpoint", endpoint, "Endpoint of server")
	flag.StringVar(&mode, "mode", mode, "Run mode: create, test")
	flag.IntVar(&runTimeSeconds, "runTime", runTimeSeconds, "Run time in seconds")

	flag.Parse()

	endpoints := strings.Split(endpoint, ",")
	rand.Seed(time.Now().UnixNano())
	fmt.Printf("Endpoints:\n")
	for i, e := range endpoints {
		fmt.Printf("%d %s\n", i, e)
	}
	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: endpoints,
		ConnLimit: 64,
	})
	if err != nil {
		fmt.Printf("Could not create connection: %v\n", err)
		os.Exit(1)
	}
	client, err := driver.NewClient(driver.ClientConfig{
		Connection: conn,
	})
	if err != nil {
		fmt.Printf("Could not create client: %v\n", err)
		os.Exit(2)
	}
	db, err := client.Database(context.Background(), "_system")
	if err != nil {
		fmt.Printf("Could not open system database: %v\n", err)
		os.Exit(3)
	}

	switch mode {
	case "create":
		if setup(drop, db) != nil {
			os.Exit(4) // Error already written
		}
		if setupSomeTenants(firstTenant, lastTenant, nrPathsPerTenant, parallelism, db) != nil {
			os.Exit(5) // Error already written
		}
	case "test":
		if runRandomTest(db, runTimeSeconds, firstTenant, lastTenant, nrPathsPerTenant, parallelism) != nil {
			os.Exit(6) // Error already written
		}
	}
}
