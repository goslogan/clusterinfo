/*
endpoints.go provides a parser for the node information in the rladmin output
Copyright © 2024 Nic Gibson <nic.gibson@redis.com>
*/
package clusterinfo

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/goslogan/fw"
)

type Endpoint struct {
	Id             string       `column:"ID" json:"id" csv:"endpointId"`
	DBId           string       `column:"DB:ID" json:"dbId" csv:"dbid"`
	Name           string       `column:"NAME" json:"name" csv:"name"`
	Node           string       `column:"NODE" json:"node" csv:"node"`
	Role           string       `column:"ROLE" json:"role" csv:"endpointRole"`
	SSL            bool         `column:"SSL" json:"ssl" csv:"ssl"`
	WatchdogStatus string       `column:"WATCHDOG_STATUS" json:"watchdogStatus" csv:"watchDogStatus"`
	TimeStamp      time.Time    `json:"timeStamp" csv:"timeStamp" column:"-"`
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
			e.TimeStamp = parent.TimeStamp
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
