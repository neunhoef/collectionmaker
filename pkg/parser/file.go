package parser

import (
	"bufio"
	"os"
)

// ReadFile
func ReadFile(filename string, d Databases, parser ParseObject) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		DBName, ColName, ShardName, shard, err := parser.GetObject(scanner.Text())
		if err != nil {
			continue
		}

		if _, ok := d[DBName]; !ok {
			database := Database{
				Collections: make(map[string]Collection),
			}
			d[DBName] = database
		}

		if _, ok := d[DBName].Collections[ColName]; !ok {
			coll := Collection{
				Shards: make(map[string]Shard),
			}
			d[DBName].Collections[ColName] = coll
		}

		currentShard, ok := d[DBName].Collections[ColName].Shards[ShardName]
		if !ok {
			d[DBName].Collections[ColName].Shards[ShardName] = Shard{
				Size:  shard.Size,
				Count: shard.Count,
			}
			continue
		}

		currentShard.Size += shard.Size
		currentShard.Count += shard.Count
		d[DBName].Collections[ColName].Shards[ShardName] = currentShard
	}

	return scanner.Err()
}
