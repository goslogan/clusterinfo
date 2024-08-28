/*
Copyright © 2024 Nic Gibson <nic.gibson@redis.com>
*/
package main_test

import (
	"bytes"
	_ "embed"

	"github.com/goslogan/rlatool/clusterinfo"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

//go:embed testdata/node_1.rladmin
var rladmin []byte

var _ = Describe("Chunks", func() {
	It("can parse into chunk", func() {
		buffer := bytes.NewReader(rladmin)
		chunks := &clusterinfo.Chunks{}
		err := chunks.Parse(buffer)
		Expect(err).NotTo(HaveOccurred())
	})
})

var _ = Describe("Nodes", func() {
	var chunks *clusterinfo.Chunks
	var info = &clusterinfo.ClusterInfo{}
	BeforeEach(func() {
		buffer := bytes.NewReader(rladmin)
		chunks = &clusterinfo.Chunks{}
		err := chunks.Parse(buffer)
		Expect(err).NotTo(HaveOccurred())
	})
	It("can parse lines into  nodes", func() {
		nodes, err := chunks.ParseNodes(info)
		Expect(err).NotTo(HaveOccurred())
		Expect(nodes).To(HaveLen(13))
		Expect(nodes[0].Id).To(Equal("node:1"))
		Expect(nodes[0].Masters + nodes[0].Replicas).To(Equal(nodes[0].ShardUsage.InUse))
		Expect(nodes[0].ShardUsage.InUse).To(Equal(uint16(94)))
		Expect(float64(nodes[0].RedisRAM.Free)).To(BeNumerically("~", 53.29, 0.1))
	})
})

var _ = Describe("Databases", func() {
	var chunks *clusterinfo.Chunks
	var info = &clusterinfo.ClusterInfo{}

	BeforeEach(func() {
		buffer := bytes.NewReader(rladmin)
		chunks = &clusterinfo.Chunks{}
		err := chunks.Parse(buffer)
		Expect(err).NotTo(HaveOccurred())
	})
	It("can parse lines into databaes", func() {
		dbs, err := chunks.ParseDatabases(info)
		Expect(err).NotTo(HaveOccurred())
		Expect(dbs).To(HaveLen(143))
	})
})

var _ = Describe("Shards", func() {
	var chunks *clusterinfo.Chunks
	var info = &clusterinfo.ClusterInfo{}

	BeforeEach(func() {
		buffer := bytes.NewReader(rladmin)
		chunks = &clusterinfo.Chunks{}
		err := chunks.Parse(buffer)
		Expect(err).NotTo(HaveOccurred())
	})
	It("can parse lines into shards", func() {
		result, err := chunks.ParseShards(info)
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(HaveLen(574))
	})
})

var _ = Describe("Endpoints", func() {
	var chunks *clusterinfo.Chunks
	var info = &clusterinfo.ClusterInfo{}
	BeforeEach(func() {
		buffer := bytes.NewReader(rladmin)
		chunks = &clusterinfo.Chunks{}
		err := chunks.Parse(buffer)
		Expect(err).NotTo(HaveOccurred())
	})
	It("can parse lines into endpoints", func() {
		result, err := chunks.ParseEndpoints(info)
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(HaveLen(144))
	})
})
