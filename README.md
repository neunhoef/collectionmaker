# The collection maker
Simple go tool to make a collection or smart graph with some documents.

### Building:
```
go build
```

##How to use help command:
```
Print main help:
smart-graph-maker help

Print usage for the specific command
smart-graph-maker help [subcommand]...
    Examples:
    smart-graph-maker help create 
    smart-graph-maker help create collection
``` 

##Examples:

#### Create collection with the size 10000 bytes and 10 documents and with two shards
```
smart-graph-maker create collection --endpoint "http://localhost:8529" --collection test --shards 2 --size 10000 --count 10
```

#### Create databases and collections from the output from debug-scripts (https://github.com/arangodb/debug-scripts)
```
./arangodb-debug.sh show-documents collection size > size.dat
./arangodb-debug.sh show-documents collection count > count.dat
smart-graph-maker create debugscript --endpoint "http://localhost:8529"  --sizefile size.dat --countfile count.log
```

The executable `smart-graph-maker` has the following options:

```
Usage of ./smart-graph-maker:
  -drop
    	set -drop to true to drop data before start
  -endpoint string
    	Endpoint of server (default "http://localhost:8529")
  -firstTenant int
    	Index of first tenant to create (default 1)
  -lastTenant int
    	Index of last tenant to create (default 3000)
  -mode string
    	Run mode: create, test (default "create")
  -nrPathsPerTenant int
    	Number of paths per tenant (default 10000)
  -parallelism int
    	Parallelism (default 4)
  -runTime int
    	Run time in seconds (default 30)
```

In `create` mode it creates "tenants". These are disjoint subgraphs in
the large smart graph. For each tenant, `nrPathsPerTenant` paths of the
form

```
instances/tenX:KY -> instances/tenX:LY -> instances/tenX:MY
```

where `X` is the tenant number and `Y` is the path number.

In `test` mode it runs this query on random access paths across all
tenants:

```
FOR v, e IN 2..2 OUTBOUND "%s" GRAPH "G" RETURN v
```

with start vertex some `instances/tenX:KY`, which will find exactly one 
path. Note that `-parallelism` is currently ignored in `test` mode.

I have used a cluster with 3 machines and 300 GB EBS gp2 volume each.
And then used one of the following commands on each coordinator:

```
./smart-graph-maker -firstTenant 1 -lastTenant 1000 -parallelism 9
./smart-graph-maker -firstTenant 1001 -lastTenant 2000 -parallelism 9
./smart-graph-maker -firstTenant 2001 -lastTenant 3000 -parallelism 9
```

This took a few hours to import data and took approximately 170 GB on
each DBServer on disk.

Then I ran tests like this:

```
./smart-graph-maker -mode=test -runTime=6000
```
