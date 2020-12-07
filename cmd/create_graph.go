package cmd

import (
	"context"
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/neunhoef/collectionmaker/pkg/database"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"strconv"
	"sync"
	"time"
)

const (
	defaultFirstTenant      = 1
	defaultLastTenant       = 3000
	defaultNrPathsPerTenant = 10000
	defaultParallelism      = 4
	defaultRunTime          = 30
)

var (
	cmdCreateGraph = &cobra.Command{
		Use:   "graph",
		Short: "Create graph",
		RunE:  createGraph,
	}
)

type Instance struct {
	Key      string `json:"_key"`
	TenantId string `json:"tenantId"`
	Payload  string `json:"payload"` // approx length 1400 bytes
}

type Step struct {
  Key      string `json:"_key,omitempty"`
	TenantId string `json:"tenantId"`
	From     string `json:"_from"`
	To       string `json:"_to"`
	Payload  string `json:"payload"` // approx length 700 bytes
}

func commonGraphFlags(command *cobra.Command) {
	var firstTenant, lastTenant, nrPathsPerTenant, parallelism int

	command.Flags().IntVar(&firstTenant, "firstTenant", 1, "Index of first tenant to create")
	command.Flags().IntVar(&lastTenant, "lastTenant", 3000, "Index of last tenant to create")
	command.Flags().IntVar(&nrPathsPerTenant, "nrPathsPerTenant", 10000, "Number of paths per tenant")
	command.Flags().IntVar(&parallelism, "parallelism", 4, "Parallelism")
}

func init() {
	var drop = false

	cmdCreateGraph.Flags().BoolVar(&drop, "drop", drop, "set -drop to true to drop data before start")
	commonGraphFlags(cmdCreateGraph)
}

func createGraph(cmd *cobra.Command, _ []string) error {
	drop, _ := cmd.Flags().GetBool("drop")
	firstTenant, _ := cmd.Flags().GetInt("firstTenant")
	lastTenant, _ := cmd.Flags().GetInt("lastTenant")
	nrPathsPerTenant, _ := cmd.Flags().GetInt("nrPathsPerTenant")
	parallelism, _ := cmd.Flags().GetInt("parallelism")

	db, err := _client.Database(context.Background(), "_system")
	if err != nil {
		return errors.Wrapf(err, "can not get database: %s", "_system")
	}

	if setup(drop, db) != nil {
		return errors.Wrapf(err, "setup was already launched")
	}

	if err := setupSomeTenants(firstTenant, lastTenant, nrPathsPerTenant, parallelism, db); err != nil {
		return errors.Wrapf(err, "can not setup some tenants")
	}

	return nil
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
			Payload:  database.MakeRandomString(1400),
		}
		in2 := Instance{
			Key:      tenantId + ":L" + n,
			TenantId: tenantId,
			Payload:  database.MakeRandomString(1400),
		}
		in3 := Instance{
			Key:      tenantId + ":M" + n,
			TenantId: tenantId,
			Payload:  database.MakeRandomString(1400),
		}
		st1 := Step{
			TenantId: tenantId,
			From:     "instances/" + tenantId + ":K" + n,
			To:       "instances/" + tenantId + ":L" + n,
			Payload:  database.MakeRandomString(700),
		}
		st2 := Step{
			TenantId: tenantId,
			From:     "instances/" + tenantId + ":L" + n,
			To:       "instances/" + tenantId + ":M" + n,
			Payload:  database.MakeRandomString(700),
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

// setup will set up a disjoint smart graph, if the smart graph is already
// there, it will not complain.
func setup(drop bool, db driver.Database) error {
	sg, err := db.Graph(nil, "Graph")
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
