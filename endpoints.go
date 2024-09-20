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
	Key            string       `column:"-" json:"key" csv:"key"`
	Source         string       `column:"-" json:"source" csv:"source"`
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

	if err == nil {
		for _, endpoint := range endpoints {
			endpoint.SetParent(parent)
		}
	}
	return endpoints, err
}

// SetParent overrides the parent setting for each database in the
// slice and updates the parent, key, source and timestamp files
func (endpoint *Endpoint) SetParent(c *ClusterInfo) {
	endpoint.parent = c
	endpoint.Key = c.Key
	endpoint.Source = c.Source
	endpoint.TimeStamp = c.TimeStamp
}

// SetSource overrides the default source for each database in the
// slice
func (endpoint *Endpoint) SetSource(info *ClusterInfo) {
	endpoint.Source = info.Source
}

func (endpoints Endpoints) JSON() (string, error) {
	data, err := json.Marshal(&endpoints)
	if err != nil {
		return "", err
	} else {
		return string(data), nil
	}
}

func (endpoints Endpoints) CSV(skipHeaders bool) (string, error) {
	if skipHeaders {
		return gocsv.MarshalStringWithoutHeaders(endpoints)
	} else {
		return gocsv.MarshalString(endpoints)
	}
}

// Set the parent (and associated fields) for all dbs
func (endpoints Endpoints) SetParent(info *ClusterInfo) {
	for _, endpoint := range endpoints {
		endpoint.SetParent(info)
	}
}

// Set the source only for all dbs {
func (endpoints Endpoints) SetSource(info *ClusterInfo) {
	for _, endpoint := range endpoints {
		endpoint.SetSource(info)
	}
}
