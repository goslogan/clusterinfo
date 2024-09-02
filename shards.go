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

	"github.com/gocarina/gocsv"
	"github.com/goslogan/fw"
)

type Shard struct {
	Id             string       `column:"ID" json:"id" csv:"ID"`
	DBId           string       `column:"DB:ID" json:"dbId" csv:"DB:ID"`
	Name           string       `column:"NAME" json:"name" csv:"NAME"`
	Node           string       `column:"NODE" json:"node" csv:"NODE"`
	Role           string       `column:"ROLE" json:"role" csv:"ROLE"`
	Slots          string       `column:"SLOTS" json:"slots" csv:"SLOTS"`
	UsedMemory     RAMFloat     `column:"USED_MEMORY" json:"usedMemory" csv:"USED_MEMORY"`
	BackupProgress string       `column:"BACKUP_PROGRESS" ßjson:"backupProgress" csv:"BACKUP_PROGRESS"`
	RAMFrag        RAMFloat     `column:"RAM_FRAG" json:"ramFrag" csv:"RAM_FRAG"`
	WatchdogStatus string       `column:"WATCHDOG_STATUS" json:"watchdogStatus" csv:"WATCHDOG_STATUS"`
	Status         string       `column:"STATUS" json:"status" csv:"STATUS"`
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
		}
	}

	return shards, err
}

func (s Shards) CSV() (string, error) {
	return gocsv.MarshalString(s)
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
