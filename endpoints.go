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
	ComponentBase
	Id             string `column:"ID" json:"id" csv:"endpointId"`
	DBId           string `column:"DB:ID" json:"dbId" csv:"dbid"`
	Name           string `column:"NAME" json:"name" csv:"name"`
	Node           string `column:"NODE" json:"node" csv:"node"`
	Role           string `column:"ROLE" json:"role" csv:"endpointRole"`
	SSL            bool   `column:"SSL" json:"ssl" csv:"ssl"`
	WatchdogStatus string `column:"WATCHDOG_STATUS" json:"watchdogStatus" csv:"watchDogStatus"`
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

// Set the parent (and associated fields) for all endpoints
func (endpoints Endpoints) SetParent(info *ClusterInfo) {
	for _, endpoint := range endpoints {
		endpoint.SetParent(info)
	}
}

// Set the source only for all dbs {
func (endpoints Endpoints) SetSource(source string) {
	for _, endpoint := range endpoints {
		endpoint.Source = source
	}
}
