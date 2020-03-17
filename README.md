# chubaodb

As a cloud native structured data store, ChubaoDB has several key features:

* flexible data model

* disaggregated storage and compute architecture

* rich secondary indexes


## External Interface

collection, document

dockey = (hashkey:string, sortkey:string), and sortkey can be empty

document API: Create, Update, Upsert, Delete, Overwrite, Get, Search, Count, ...


## Architecture

master, partition server, router

orchestracted by Kubernetes

ChubaoFS (replicated across three AZs) works as the shared storage infrastructure


## Licence

Apache 2


## Acknowledgments

rocksdb


