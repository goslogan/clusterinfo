/*
chunks.go provides a base parser that loads rladmin output and parses it into chunks for each section.
Copyright © 2024 Nic Gibson <nic.gibson@redis.com>
*/
package clusterinfo

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
)

// Chunks is used to store the output of the base parser.
type Chunks struct {
	Intro     string
	Nodes     []byte
	Databases []byte
	Endpoints []byte
	Shards    []byte
}

var marker = regexp.MustCompile(`^([A-Z ]+):$`)

const (
	ChunkNone = iota
	ChunkCluster
	ChunkNodes
	ChunkDatabases
	ChunkEndpoints
	ChunkShards
)

var chunkMap = map[string]int{
	"CLUSTER":       ChunkCluster,
	"CLUSTER NODES": ChunkNodes,
	"DATABASES":     ChunkDatabases,
	"ENDPOINTS":     ChunkEndpoints,
	"SHARDS":        ChunkShards,
}

func (c *Chunks) Parse(input io.Reader) error {

	current := make([]byte, 0)

	where := ChunkNone
	scanner := bufio.NewScanner(input)

	for scanner.Scan() {
		line := scanner.Bytes()
		if newChunk := c.whichChunk(line); newChunk != ChunkNone {
			c.putData(current, where)
			where = newChunk
			current = make([]byte, 0)
		} else {
			line := append(bytes.TrimSpace(line), '\n')
			current = append(current, line...)
		}
	}

	if scanner.Err() != nil {
		return scanner.Err()
	} else {
		c.putData(current, where)
	}

	return nil
}

func (c *Chunks) putData(data []byte, stage int) {
	if len(data) > 0 {
		switch stage {
		case ChunkNone:
			c.Intro = c.Intro + "\n" + string(data) // Don't convert this
		case ChunkNodes:
			c.Nodes = data
		case ChunkDatabases:
			c.Databases = data
		case ChunkEndpoints:
			c.Endpoints = data
		case ChunkShards:
			c.Shards = data
		}
	}
}

// ExtractTimeStamp finds the timestamp at the start of the output and returns it as time.Time
func (c *Chunks) ExtractTimeStamp() (time.Time, error) {

	lines := strings.Split(c.Intro, "\n")
	if len(lines) < 2 {
		return time.Now(), fmt.Errorf("rlatool - timestamp not found in input")
	} else {
		return time.Parse("2006-01-02 03:04:05.000000-07:00", lines[1])
	}

}

// Get the id of the chunk we've encountered
func (c *Chunks) whichChunk(line []byte) int {

	matched := marker.FindSubmatch(line)
	if len(matched) <= 1 {
		return ChunkNone
	} else {
		if which, ok := chunkMap[string(matched[1])]; ok {
			return which
		} else {
			return ChunkNone
		}
	}

}
