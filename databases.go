/*
databases.go provides a parser for the database information in the rladmin output
Copyright © 2024 Nic Gibson <nic.gibson@redis.com>
*/
package clusterinfo

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/goslogan/fw"
)

type DBEndPoints []string

type DBShards struct {
	Masters  uint16
	Replicas uint16
}
type DBNodes map[string]*DBShards

type Database struct {
	Key               string       `column:"-" json:"key" csv:"key"`
	Source            string       `column:"-" json:"source" csv:"source"`
	Id                string       `column:"DB:ID" json:"id" csv:"id"`
	Name              string       `column:"NAME" json:"name" csv:"name"`
	Type              string       `column:"TYPE" json:"type" csv:"type"`
	Status            string       `column:"STATUS" json:"status" csv:"status"`
	MasterShards      uint16       `column:"SHARDS" json:"shards" csv:"shards"`
	Placement         string       `column:"PLACEMENT" json:"placement" csv:"placement"`
	Replication       string       `column:"REPLICATION" json:"replication" csv:"replication"`
	Persistence       string       `column:"PERSISTENCE" json:"persistence" csv:"persistence"`
	Endpoint          DBEndPoints  `column:"ENDPOINT" json:"endpoints" csv:"endpoints"`
	ExecState         string       `column:"EXEC_STATE" json:"execState" csv:"execState"`
	ExecStateMachine  string       `column:"EXEC_STATE_MACHINE" json:"execStateMachine" csv:"execStateMachine"`
	BackupProgress    string       `column:"BACKUP_PROGRESS" json:"backupProgress" csv:"backupProgress"`
	MissingBackupTime string       `column:"MISSING_BACKUP_TIME" json:"missingBackupTime" csv:"missingBackupTime"`
	RedisVersion      string       `column:"REDIS_VERSION" json:"redisVersion" csv:"redisVersion"`
	TimeStamp         time.Time    `json:"timeStamp" csv:"timeStamp" column:"-"`
	parent            *ClusterInfo `json:"-" csv:"-"`
}

type Databases []*Database

func (c *Chunks) ParseDatabases(parent *ClusterInfo) (Databases, error) {

	databases := []*Database{}
	decoder := fw.NewDecoder(bytes.NewReader(c.Databases))
	decoder.IgnoreEmptyRecords = true

	err := decoder.Decode(&databases)

	if err != nil {
		return nil, err
	}
	for _, db := range databases {
		db.SetParent(parent)
	}

	return databases, nil
}

// SetParent overrides the parent setting for each database in the
// slice and updates the parent, key, source and timestamp files
func (db *Database) SetParent(c *ClusterInfo) {
	db.parent = c
	db.Key = c.Key
	db.Source = c.Source
	db.TimeStamp = c.TimeStamp
}

// SetSource overrides the default source for each database in the
// slice
func (db *Database) SetSource(info *ClusterInfo) {
	db.Source = info.Source
}

// JSON returns the database struct marsalled to JSON
func (db *Database) JSON() (string, error) {
	if out, err := json.Marshal(db); err != nil {
		return "", err
	} else {
		return string(out), nil
	}
}

func (d Databases) JSON() (string, error) {
	if out, err := json.Marshal(d); err != nil {
		return "", err
	} else {
		return string(out), nil
	}
}

// Marshal the databases to a string and return it.
// If the skipHeaders parameter is true, marshall without headers
func (d Databases) CSV(skipHeaders bool) (string, error) {
	if skipHeaders {
		return gocsv.MarshalStringWithoutHeaders(d)
	} else {
		return gocsv.MarshalString(d)
	}
}

// Set the parent (and associated fields) for all dbs
func (d Databases) SetParent(info *ClusterInfo) {
	for _, db := range d {
		db.SetParent(info)
	}
}

// Set the source only for all dbs {
func (d Databases) SetSource(info *ClusterInfo) {
	for _, db := range d {
		db.SetSource(info)
	}
}

func (n *DBNodes) MarshalCSV() (string, error) {

	keys := []string{}

	for k, v := range *n {
		if v.Masters+v.Replicas > 0 {
			keys = append(keys, k)
		}
	}

	return strings.Join(keys, "/"), nil
}
