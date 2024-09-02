/*
Copyright © 2024 Nic Gibson <nic.gibson@redis.com>
*/
package clusterinfo

import (
	"bytes"
	_ "embed"
	"testing"

	"github.com/stretchr/testify/assert"
)

//go:embed testdata/node_1.rladmin
var rladmin []byte

func TestChunking(t *testing.T) {
	buffer := bytes.NewReader(rladmin)
	chunks := Chunks{}
	err := chunks.Parse(buffer)
	if assert.Nil(t, err) {
		assert.NotEmpty(t, chunks.Nodes)
		assert.NotEmpty(t, chunks.Databases)
		assert.NotEmpty(t, chunks.Endpoints)
		assert.NotEmpty(t, chunks.Shards)
	}
}

func TestNodes(t *testing.T) {
	var chunks *Chunks
	var info = &ClusterInfo{}

	buffer := bytes.NewReader(rladmin)
	chunks = &Chunks{}
	err := chunks.Parse(buffer)
	if assert.Nil(t, err) {
		nodes, err := chunks.ParseNodes(info)
		if assert.Nil(t, err) {
			assert.Len(t, nodes, 13)
			assert.Equal(t, nodes[0].Id, "node:1")
			assert.Equal(t, nodes[0].Masters+nodes[0].Replicas, nodes[0].ShardUsage.InUse)
			assert.Equal(t, nodes[0].ShardUsage.InUse, uint16(94))
			assert.LessOrEqual(t, nodes[0].RedisRAM.Free, 53.24)
			assert.GreaterOrEqual(t, nodes[0].RedisRAM.Free, 53.23)
		}
	}
}

func TestDatabases(t *testing.T) {
	var chunks *Chunks
	var info = &ClusterInfo{}

	buffer := bytes.NewReader(rladmin)
	chunks = &Chunks{}
	err := chunks.Parse(buffer)
	if assert.Nil(t, err) {
		dbs, err := chunks.ParseDatabases(info)
		if assert.Nil(t, err) {
			assert.Len(t, dbs, 143)
			assert.Equal(t, dbs[0].Id, "db:10567021")
			assert.Equal(t, dbs[0].Endpoint, DBEndPoints([]string{
				"redis-17798.c99999.us-central1-mz.gcp.cloud.rlrcp.com:17798",
				"redis-17798.c99999.us-central1-mz.gcp.redns.redis-cloud.com:17798",
				"redis-17798.internal.c99999.us-central1-mz.gcp.cloud.rlrcp.com:17798"}))
		}
	}
}

func TestShards(t *testing.T) {
	var chunks *Chunks
	var info = &ClusterInfo{}

	buffer := bytes.NewReader(rladmin)
	chunks = &Chunks{}
	err := chunks.Parse(buffer)
	if assert.Nil(t, err) {
		shards, err := chunks.ParseShards(info)
		if assert.Nil(t, err) {
			assert.Len(t, shards, 574)
			assert.Equal(t, "sudan-02", shards[0].Name)
		}
	}
}

func TestEndpoints(t *testing.T) {
	var chunks *Chunks
	var info = &ClusterInfo{}

	buffer := bytes.NewReader(rladmin)
	chunks = &Chunks{}
	err := chunks.Parse(buffer)
	if assert.Nil(t, err) {
		eps, err := chunks.ParseEndpoints(info)
		if assert.Nil(t, err) {
			assert.Len(t, eps, 144)
			assert.Equal(t, eps[1].Name, "cambodia-00")
			assert.Equal(t, eps[1].Role, "single")
		}
	}
}
