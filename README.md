# chubaodb

ChubaoDB is a cloud native distributed document database on top of ChubaoFS. 

As a scalable non-relational structured data infrastructure, ChubaoDB has several key features:

* flexible data model

* disaggregated storage and compute architecture

* rich indexes for efficient search


## External Interface

collection, document

dockey = (hashkey:string, sortkey:string), and sortkey can be empty

document API: Create, Update, Upsert, Delete, Overwrite, Get, Search, Count, ...


## Architecture

master, partition server, router

orchestracted by Kubernetes

ChubaoFS (replicated across three AZs) works as the underlying storage infrastructure


## Licence

Apache 2


## Acknowledgments

* jimraft

* rocksdb

* tantivy


