/*
Copyright © 2024 Nic Gibson <nic.gibson@redis.com>
*/
package clusterinfo

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/nic-gibson/go-bytesize"
)

type Serializer interface {
	CSV() (string, error)
	JSON() (string, error)
}

// ClusterInfo represents all the data loaded from the rladmin status output
type ClusterInfo struct {
	Unparsed  *Chunks   `json:"-"`
	Databases Databases `json:"databases"`
	Endpoints Endpoints `json:"endpoints"`
	Shards    Shards    `json:"shards"`
	Nodes     Nodes     `json:"nodes"`
	TimeStamp time.Time `json:"timeStamp"`
}

type RAMFloat float64

func (g *RAMFloat) UnmarshalText(s []byte) error {
	f, err := parseMemory(string(s))
	*g = RAMFloat(f)
	return err
}

func (g *RAMFloat) MarshalText() ([]byte, error) {
	return []byte(fmt.Sprintf("%0.5f", *g)), nil
}

func toUint16(s string) (uint16, error) {
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	} else {
		return uint16(v), nil
	}

}

func parseMemory(s string) (RAMFloat, error) {
	invert := 1
	if s[0] == '-' {
		invert = -1
		s = s[1:]
	}
	v, err := bytesize.Parse(s)
	if err != nil {
		return 0, err
	} else {
		return RAMFloat(float64(invert) * v.GBytes()), nil
	}

}

func NewClusterInfo(in io.Reader) (*ClusterInfo, error) {

	info := &ClusterInfo{}

	chunks := &Chunks{}
	err := chunks.Parse(in)

	if err != nil {
		return nil, err
	} else {
		info.Unparsed = chunks

	}

	info.Databases, err = chunks.ParseDatabases(info)
	if err != nil {
		return nil, err
	}

	info.Shards, err = chunks.ParseShards(info)
	if err != nil {
		return nil, err
	}

	info.Endpoints, err = chunks.ParseEndpoints(info)
	if err != nil {
		return nil, err
	}

	info.Nodes, err = chunks.ParseNodes(info)
	if err != nil {
		return nil, err
	}

	ts, err := chunks.ExtractTimeStamp()
	if err == nil {
		info.TimeStamp = ts
	}

	return info, nil
}

func (c *ClusterInfo) DatabasesWithNodes() DatabasesWithNodes {
	return c.Databases.withNodes()
}

func (c *ClusterInfo) JSON() (string, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return "", err
	} else {
		return string(data), nil
	}
}
