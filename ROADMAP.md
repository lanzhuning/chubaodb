# Engineering Roadmap of ChubaoDB

This file describes ChubaoDB's development plan. 

* basic framework

master, pserver, router, definitions of the RPC services and protos

document storage in rocksdb, PUT/GET, no secondary indexing

test on CFS vs. on local fs

* pserver: richer write operations and _latch_ mechanism

insert/update/upsert/delete, and conditional write

efficient latch implementation


* pserver: async secondary and full-text indexing

simba = rocksdb, latch, tantivy

the indexer daemon


* master

core interfaces: CreateCollection(), CreatePartition(), AddServer()

core data structures: ZoneInfo, ServerInfo, PartitionInfo, CollectionInfo

persistence: rocksdb on chubaofs, sync write mode

availability: orchestrated by kubernetes

core algo logics: how to select servers when creating a partition

the key point: how to avoid orphan partitions

* partition transfer between pservers

on top of ChubaoFS, pservers decouple compute from storage. 

partition transfer: master tells pserver(A) to offload partition(P) and pserver(B) to load partition(P)

add two new intefaces to pserver: offloadPartition(), loadPartition()

offloadPartition(): 
1, remove from the partition map
2, await all write requests to be finished - saved to rocksdb + indexed
3, report to the master


* router

write, delete, search (from one partition or all partitions)

* failure detection and auto failover

* kubernetes integration

* dynamic load balancing

intelligent scheduling of partitions among pservers

* cluster management

repl topo graph


* cspark


