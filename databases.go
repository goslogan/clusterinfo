/*
databases.go provides a parser for the database information in the rladmin output
Copyright © 2024 Nic Gibson <nic.gibson@redis.com>
*/
package clusterinfo

import (
	"bytes"
	"encoding/json"
	"strings"

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
	ComponentBase
	Id                string      `column:"DB:ID" json:"id" csv:"id"`
	Name              string      `column:"NAME" json:"name" csv:"name"`
	Type              string      `column:"TYPE" json:"type" csv:"type"`
	Status            string      `column:"STATUS" json:"status" csv:"status"`
	MasterShards      uint16      `column:"SHARDS" json:"shards" csv:"shards"`
	Placement         string      `column:"PLACEMENT" json:"placement" csv:"placement"`
	Replication       string      `column:"REPLICATION" json:"replication" csv:"replication"`
	Persistence       string      `column:"PERSISTENCE" json:"persistence" csv:"persistence"`
	Endpoint          DBEndPoints `column:"ENDPOINT" json:"endpoints" csv:"endpoints"`
	ExecState         string      `column:"EXEC_STATE" json:"execState" csv:"execState"`
	ExecStateMachine  string      `column:"EXEC_STATE_MACHINE" json:"execStateMachine" csv:"execStateMachine"`
	BackupProgress    string      `column:"BACKUP_PROGRESS" json:"backupProgress" csv:"backupProgress"`
	MissingBackupTime string      `column:"MISSING_BACKUP_TIME" json:"missingBackupTime" csv:"missingBackupTime"`
	RedisVersion      string      `column:"REDIS_VERSION" json:"redisVersion" csv:"redisVersion"`
}

type Databases []*Database

func (c *Chunks) ParseDatabases(parent *ClusterInfo) (Databases, error) {

	databases := Databases{}
	decoder := fw.NewDecoder(bytes.NewReader(c.Databases))
	decoder.IgnoreEmptyRecords = true

	err := decoder.Decode(&databases)

	if err != nil {
		return nil, err
	}

	clusterName := databases.ClusterName()

	for _, db := range databases {
		db.Source = clusterName
		db.SetParent(parent)
	}

	return databases, nil
}

// ClusterName builds a cluster name from the database endpoints and returns it
func (db *Database) ClusterName() string {
	if len(db.Endpoint) == 0 { // no endpoints
		return ""
	} else {
		components := strings.Split(db.Endpoint[0], ":")
		if len(components) == 0 { // no addr:host
			return ""
		} else {
			names := strings.Split(components[0], ".")
			return strings.Join(names[1:], ".")
		}
	}
}

// ClusterName gets the cluster name from the first database in the list
func (dbs Databases) ClusterName() string {
	if len(dbs) == 0 {
		return ""
	} else {
		return dbs[0].ClusterName()
	}
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
func (d Databases) SetSource(source string) {
	for _, db := range d {
		db.Source = source
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

func (db *DBEndPoints) UnmarshalText(text []byte) error {
	dbs := DBEndPoints(strings.Split(string(text), "/"))
	*db = dbs
	return nil
}
