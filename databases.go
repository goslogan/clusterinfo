/*
databases.go provides a parser for the database information in the rladmin output
Copyright © 2024 Nic Gibson <nic.gibson@redis.com>
*/
package clusterinfo

import (
	"encoding/json"
	"strings"

	"github.com/gocarina/gocsv"
	"github.com/o1egl/fwencoder"
)

type DBEndPoints []string

type DBShards struct {
	Masters  uint16
	Replicas uint16
}
type DBNodes map[string]*DBShards

type Database struct {
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
	parent            *ClusterInfo `json:"-" csv:"-"`
}

type DatabaseWithNodes struct {
	Database
	Nodes DBNodes `json:"nodes" csv:"nodes" column:"NODES"`
}

type Databases []*Database
type DatabasesWithNodes []*DatabaseWithNodes

func (c *Chunks) ParseDatabases(parent *ClusterInfo) (Databases, error) {

	databases := []*Database{}

	err := fwencoder.Unmarshal(c.Databases, databases)

	if err != nil {
		return nil, err
	}
	for _, db := range databases {
		db.parent = parent
	}

	return databases, nil
}

// JSON returns the database struct marsalled to JSON
func (db *Database) JSON() (string, error) {
	if out, err := json.Marshal(db); err != nil {
		return "", err
	} else {
		return string(out), nil
	}
}

// OnNode returns the number of shards on the given node for a database.
func (db *Database) OnNode(id string) DBShards {
	var masters, replicas uint16
	for _, shard := range db.parent.Shards.ForDB(db.Id) {
		if shard.Node == id {
			if shard.Role == "master" {
				masters++
			} else {
				replicas++
			}
		}
	}

	return DBShards{Masters: masters, Replicas: replicas}
}

func (d *Database) withNodes() *DatabaseWithNodes {
	nodes := DBNodes{}

	for _, node := range d.parent.Nodes {
		nodes[node.Id] = &DBShards{}
	}

	for _, shard := range d.parent.Shards {
		if shard.DBId == d.Id {
			shardCount := nodes[shard.Node]

			if shard.Role == "master" {
				shardCount.Masters++
			} else {
				shardCount.Replicas++
			}
		}
	}

	return &DatabaseWithNodes{
		Database: *d,
		Nodes:    d.getNodes(),
	}
}

// ShardCount returns the total number of shards by
// counting them.
func (d *Database) ShardCount() uint16 {
	shards := uint16(0)
	for _, v := range d.getNodes() {
		shards += v.Masters
		shards += v.Replicas
	}

	return shards

}

func (d *Database) getNodes() DBNodes {
	nodes := DBNodes{}

	for _, node := range d.parent.Nodes {
		nodes[node.Id] = &DBShards{}
	}

	for _, shard := range d.parent.Shards {
		if shard.DBId == d.Id {
			shardCount := nodes[shard.Node]

			if shard.Role == "master" {
				shardCount.Masters++
			} else {
				shardCount.Replicas++
			}
		}
	}

	return nodes
}

func (d *Databases) JSON() (string, error) {
	if out, err := json.Marshal(d); err != nil {
		return "", err
	} else {
		return string(out), nil
	}
}

func (d Databases) CSV() (string, error) {
	return gocsv.MarshalString(d)
}

func (d DatabasesWithNodes) JSON() (string, error) {
	if out, err := json.Marshal(d); err != nil {
		return "", err
	} else {
		return string(out), nil
	}
}

func (d DatabasesWithNodes) CSV() (string, error) {
	return gocsv.MarshalString(d)
}

func (d Databases) withNodes() DatabasesWithNodes {
	dn := DatabasesWithNodes{}
	for _, db := range d {
		dn = append(dn, db.withNodes())
	}

	return dn
}

func (e *DBEndPoints) UnmarshalCSV(input string) error {
	tmp := DBEndPoints(strings.Split(input, "/"))
	*e = tmp
	return nil
}

func (e *DBEndPoints) MarshalCSV() (string, error) {
	return strings.Join(*e, "/"), nil
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
