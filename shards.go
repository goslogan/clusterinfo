/*
shards.go provides a parser for the shard information in the rladmin output
Copyright © 2024 Nic Gibson <nic.gibson@redis.com>
*/
package clusterinfo

import (
	"cmp"
	"encoding/json"
	"slices"

	"github.com/gocarina/gocsv"
	"github.com/o1egl/fwencoder"
)

type Shard struct {
	Id             string       `json:"id" csv:"ID"`
	DBId           string       `json:"dbId" csv:"DB:ID"`
	Name           string       `json:"name" csv:"NAME"`
	Node           string       `json:"node" csv:"NODE"`
	Role           string       `json:"role" csv:"ROLE"`
	Slots          string       `json:"slots" csv:"SLOTS"`
	UsedMemory     RAMFloat     `json:"usedMemory" csv:"USED_MEMORY"`
	BackupProgress string       `json:"backupProgress" csv:"BACKUP_PROGRESS"`
	RAMFrag        RAMFloat     `json:"ramFrag" csv:"RAM_FRAG"`
	WatchdogStatus string       `json:"watchdogStatus" csv:"WATCHDOG_STATUS"`
	Status         string       `json:"status" csv:"STATUS"`
	parent         *ClusterInfo `csv:"-" json:"-"`
}

type Shards []*Shard

func (c *Chunks) ParseShards(parent *ClusterInfo) (Shards, error) {
	shards := Shards{}

	err := fwencoder.Unmarshal(c.Shards, shards)

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
