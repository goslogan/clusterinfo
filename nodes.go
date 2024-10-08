/*
nodes.go provides a parser for the node information in the rladmin output
Copyright © 2024 Nic Gibson <nic.gibson@redis.com>
*/
package clusterinfo

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/goslogan/fw"
)

const (
	errorString = "unable to parse '%s' as %s: %w"
)

type IP struct {
	net.IP
}

type ShardInfo struct {
	InUse uint16 `json:"shardsInUse"`
	Max   uint16 `json:"maxShards"`
}

type MemoryInfo struct {
	Free RAMFloat `json:"free"`
	Max  RAMFloat `json:"max"`
}

type Node struct {
	Key              string       `columh:"-" json:"key" csv:"key"`
	Id               string       `json:"nodeId" csv:"nodeId" column:"NODE:ID" `
	Role             string       `json:"role" csv:"role" column:"ROLE"`
	Address          IP           `json:"address" csv:"address" column:"ADDRESS"`
	ExternalAddress  IP           `json:"externalAddress" csv:"externalAddress" column:"EXTERNAL_ADDRESS"`
	HostName         string       `json:"hostName" csv:"hostName" column:"HOSTNAME"`
	OverbookingDepth RAMFloat     `json:"overbookingDepth" csv:"overbookingDepth" column:"OVERBOOKING_DEPTH"`
	Masters          uint16       `json:"masters" csv:"masters" column:"MASTERS"`
	Replicas         uint16       `json:"replicas" csv:"replicas" column:"SLAVES"`
	ShardUsage       ShardInfo    `json:"shards" csv:"shards" column:"SHARDS"`
	Cores            uint16       `json:"cores" csv:"cores" column:"CORES"`
	RedisRAM         MemoryInfo   `json:"redisRAM" csv:"redisRAM" column:"FREE_RAM"`
	ProvisionalRAM   MemoryInfo   `json:"provisionalRAM" csv:"provisionalRAM" column:"PROVISIONAL_RAM"`
	Version          string       `json:"version" csv:"version" column:"VERSION"`
	SHA              string       `json:"sha" csv:"sha" column:"SHA"`
	RackId           string       `json:"rackId" csv:"rackId" column:"RACK-ID"`
	Status           string       `json:"status" csv:"status" column:"STATUS"`
	Quorum           bool         `json:"quorum" csv:"quorum" column:"-"`
	TimeStamp        time.Time    `json:"timeStamp" csv:"timeStamp" column:"-"`
	parent           *ClusterInfo `csv:"-" column:"-"`
}

type Nodes []*Node

func (c *Chunks) ParseNodes(parent *ClusterInfo) (Nodes, error) {

	nodes := []*Node{}

	decoder := fw.NewDecoder(bytes.NewReader(c.Nodes))
	decoder.IgnoreEmptyRecords = true

	err := decoder.Decode(&nodes)

	if err != nil {
		return nil, err
	}

	for _, node := range nodes {
		node.parent = parent
		node.Key = parent.Key
		node.TimeStamp = parent.TimeStamp
		if node.ShardUsage.Max == 0 {
			node.Quorum = true
		}
		// strip of the "*" prefix from the node that ran the rladmin status
		node.Id = strings.TrimPrefix(node.Id, "*")
	}

	return nodes, nil
}

func (m *MemoryInfo) UnmarshalText(input []byte) error {
	if parts := strings.Split(string(input), "/"); len(parts) == 2 {
		f, err := parseMemory(parts[0])
		if err != nil {
			return fmt.Errorf(errorString, "memory info", parts[0], err)
		} else {
			m.Free = RAMFloat(f)
		}
		f, err = parseMemory(parts[1])
		if err != nil {
			return fmt.Errorf(errorString, "memory info", parts[1], err)
		} else {
			m.Max = RAMFloat(f)
		}
	} else {
		return fmt.Errorf("unable to split %s into parts for memory info", input)
	}

	return nil
}

func (i *IP) UnmarshalText(input []byte) error {

	if len(input) == 0 {
		return nil
	}

	i.IP = net.ParseIP(string(input))
	if i.IP == nil {
		return fmt.Errorf(errorString, "address", input, nil)
	} else {
		return nil
	}
}

func (s *ShardInfo) UnmarshalCSV(input string) error {
	var err error
	if parts := strings.Split(input, "/"); len(parts) == 2 {
		if s.InUse, err = toUint16(parts[0]); err != nil {
			return fmt.Errorf(errorString, "number of shards", parts[0], err)
		}
		if s.Max, err = toUint16(parts[1]); err != nil {
			return fmt.Errorf(errorString, "maximum number of shards", parts[1], err)
		}
	} else {
		return fmt.Errorf("unable to split %s into parts for shard counts", input)
	}

	return nil
}

func (s *ShardInfo) UnmarshalText(input []byte) error {
	return s.UnmarshalCSV(string(input))
}

func (ns Nodes) JSON() (string, error) {
	data, err := json.Marshal(&ns)
	if err != nil {
		return "", err
	} else {
		return string(data), nil
	}
}

func (ns Nodes) CSV(skipHeaders bool) (string, error) {
	if skipHeaders {
		return gocsv.MarshalStringWithoutHeaders(ns)
	} else {
		return gocsv.MarshalString(ns)
	}
}
