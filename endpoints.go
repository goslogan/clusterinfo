/*
endpoints.go provides a parser for the node information in the rladmin output
Copyright © 2024 Nic Gibson <nic.gibson@redis.com>
*/
package clusterinfo

import (
	"bytes"
	"encoding/json"

	"github.com/gocarina/gocsv"
	"github.com/goslogan/fw"
)

type Endpoint struct {
	Id             string       `column:"ID" json:"id" csv:"ID"`
	DBId           string       `column:"DB:ID" json:"dbId" csv:"DB:ID"`
	Name           string       `column:"NAME" json:"name" csv:"NAME"`
	Node           string       `column:"NODE" json:"node" csv:"NODE"`
	Role           string       `column:"ROLE" json:"role" csv:"ROLE"`
	SSL            bool         `column:"SSL" json:"ssl" csv:"SSL"`
	WatchdogStatus string       `column:"WATCHDOG_STATUS" json:"watchdogStatus" csv:"WATCHDOG_STATUS"`
	parent         *ClusterInfo `csv:"-" json:"-"`
}

type Endpoints []*Endpoint

func (c *Chunks) ParseEndpoints(parent *ClusterInfo) (Endpoints, error) {
	endpoints := []*Endpoint{}
	decoder := fw.NewDecoder(bytes.NewReader(c.Endpoints))
	decoder.IgnoreEmptyRecords = true

	err := decoder.Decode(&endpoints)

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
