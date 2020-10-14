package parser

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	Size  = iota
	Count = iota
)

type DatabaseMetaDataFromDebugScript struct {
	SizeFileName  string
	CountFileName string
}

type TextLine struct {
	ObjectType int
}

// GetObject parse one line from the source which is produced by `https://github.com/arangodb/debug-scripts`.
// The input from debug script should like:
// | dbname | collection | shard name | total | dbserver1 | .... |
func (t TextLine) GetObject(input string) (DBName string, ColName string, ShardName string, shard Shard, err error) {
	fields := strings.Split(input, "|")
	if len(fields) < 2 {
		err = fmt.Errorf("too little fields int the line")
		return
	}

	fields = fields[1 : len(fields)-1]

	if len(fields) < 5 {
		err = fmt.Errorf("too little fields int the line")
		return
	}

	DBName = strings.TrimSpace(fields[0])
	ColName = strings.TrimSpace(fields[1])
	ShardName = strings.TrimSpace(fields[2])
	if len(ShardName) == 0 || ShardName[0] != 's' {
		err = fmt.Errorf("invalid shard name: %s", ShardName)
		return
	}

	var total, value int
	total, err = strconv.Atoi(strings.TrimSpace(fields[3]))
	if err != nil {
		return
	}

	if total == 0 {
		shard.ReplicationFactor = 1
		return
	}

	replicationFactor := 0
	for _, field := range fields[4:] {
		value, err = strconv.Atoi(strings.TrimSpace(field))
		if err != nil {
			return
		}
		if value > 0 {
			replicationFactor++
		}
	}

	if replicationFactor == 0 {
		shard.ReplicationFactor = 1
	} else {
		shard.ReplicationFactor = replicationFactor
	}

	if t.ObjectType == Size {
		shard.Size = total / shard.ReplicationFactor
	} else {
		shard.Count = total / shard.ReplicationFactor
	}

	return
}

func (s *DatabaseMetaDataFromDebugScript) GetData() (Databases, error) {
	databases := make(Databases)

	textLineSize := TextLine{
		ObjectType: Size,
	}
	if err := ReadFile(s.SizeFileName, databases, &textLineSize); err != nil {
		return nil, err
	}

	textLineCount := TextLine{
		ObjectType: Count,
	}

	if err := ReadFile(s.CountFileName, databases, &textLineCount); err != nil {
		return nil, err
	}

	return databases, nil
}
