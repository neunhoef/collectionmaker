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
func (t TextLine) GetObject(input string) (DBName string, ColName string, ShardName string, shard Shard, err error) {
	fields := strings.Split(input, "|")
	if len(fields) < 5 {
		err = fmt.Errorf("too little fields int the line")
		return
	}

	fields = fields[1:]
	for len(fields[0]) == 0 {
		fields = fields[1:]
	}

	if len(fields) < 4 {
		err = fmt.Errorf("too little fields int the line")
		return
	}

	ShardName = strings.TrimSpace(fields[2])
	if len(ShardName) == 0 || ShardName[0] != 's' {
		err = fmt.Errorf("invalid shard name: %s", ShardName)
		return
	}
	DBName = strings.TrimSpace(fields[0])
	ColName = strings.TrimSpace(fields[1])

	var value int
	value, err = strconv.Atoi(strings.TrimSpace(fields[3]))
	if err != nil {
		return
	}

	if t.ObjectType == Size {
		shard.Size = value
	} else {
		shard.Count = value
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
