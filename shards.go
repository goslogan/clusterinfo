/*
shards.go provides a parser for the shard information in the rladmin output
Copyright © 2024 Nic Gibson <nic.gibson@redis.com>
*/
package clusterinfo

import (
	"bytes"
	"cmp"
	"encoding/json"
	"slices"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/goslogan/fw"
)

type Shard struct {
	Key            string       `columh:"-" json:"key" csv:"key"`
	Id             string       `column:"ID" json:"id" csv:"shardid"`
	DBId           string       `column:"DB:ID" json:"dbId" csv:"dbid"`
	Name           string       `column:"NAME" json:"name" csv:"name"`
	Node           string       `column:"NODE" json:"node" csv:"node"`
	Role           string       `column:"ROLE" json:"role" csv:"role"`
	Slots          string       `column:"SLOTS" json:"slots" csv:"slots"`
	UsedMemory     RAMFloat     `column:"USED_MEMORY" json:"usedMemory" csv:"usedMemory"`
	BackupProgress string       `column:"BACKUP_PROGRESS" ßjson:"backupProgress" csv:"backupProgress"`
	RAMFrag        RAMFloat     `column:"RAM_FRAG" json:"ramFrag" csv:"ramFrag"`
	WatchdogStatus string       `column:"WATCHDOG_STATUS" json:"watchdogStatus" csv:"watchdogStatus"`
	Status         string       `column:"STATUS" json:"status" csv:"status"`
	TimeStamp      time.Time    `json:"timeStamp" csv:"timeStamp" column:"-"`
	parent         *ClusterInfo `csv:"-" json:"-"`
}

type Shards []*Shard

func (c *Chunks) ParseShards(parent *ClusterInfo) (Shards, error) {
	shards := Shards{}

	decoder := fw.NewDecoder(bytes.NewReader(c.Shards))
	decoder.IgnoreEmptyRecords = true

	err := decoder.Decode(&shards)
	if err == nil {
		for _, s := range shards {
			s.parent = parent
			s.Key = parent.Key
			s.TimeStamp = parent.TimeStamp
		}
	}

	return shards, err
}

func (s Shards) CSV(skipHeaders bool) (string, error) {
	if skipHeaders {
		return gocsv.MarshalStringWithoutHeaders(s)
	} else {
		return gocsv.MarshalString(s)
	}
}

func (s Shards) JSON() (string, error) {
	if out, err := json.Marshal(s); err != nil {
		return "", err
	} else {
		return string(out), nil
	}
}

// ForDB returns all the shards for a given database, sorted in
// Id order
func (s Shards) ForDB(id string) Shards {
	ds := make(Shards, 0)
	for _, shard := range s {
		if shard.DBId == id {
			ds = append(ds, shard)
		}
	}

	slices.SortStableFunc(ds, func(a *Shard, b *Shard) int {
		return cmp.Compare(a.Id, b.Id)
	})

	return ds

}
