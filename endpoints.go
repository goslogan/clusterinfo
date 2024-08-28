/*
endpoints.go provides a parser for the node information in the rladmin output
Copyright © 2024 Nic Gibson <nic.gibson@redis.com>
*/
package clusterinfo

import (
	"encoding/json"

	"github.com/gocarina/gocsv"
	"github.com/o1egl/fwencoder"
)

type Endpoint struct {
	Id             string       `json:"id" csv:"ID"`
	DBId           string       `json:"dbId" csv:"DB:ID"`
	Name           string       `json:"name" csv:"NAME"`
	Node           string       `json:"node" csv:"NODE"`
	Role           string       `json:"role" csv:"ROLE"`
	SSL            bool         `json:"ssl" csv:"SSL"`
	WatchdogStatus string       `json:"watchdogStatus" csv:"WATCHDOG_STATUS"`
	parent         *ClusterInfo `csv:"-" json:"-"`
}

type Endpoints []*Endpoint

func (c *Chunks) ParseEndpoints(parent *ClusterInfo) (Endpoints, error) {
	endpoints := []*Endpoint{}

	err := fwencoder.Unmarshal(c.Endpoints, endpoints)

	if err != nil {
		for _, e := range endpoints {
			e.parent = parent
		}
	}
	return endpoints, err
}

func (e Endpoints) JSON() (string, error) {
	data, err := json.Marshal(&e)
	if err != nil {
		return "", err
	} else {
		return string(data), nil
	}
}

func (e Endpoints) CSV() (string, error) {
	return gocsv.MarshalString(&e)
}
