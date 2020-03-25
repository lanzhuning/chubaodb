// Copyright 2020 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

use crate::pserver::simba::engine::{
    engine::{BaseEngine, Engine},
    rocksdb::RocksDB,
    tantivy::Tantivy,
};
use crate::pserver::simba::latch::Latch;
use crate::pserverpb::*;
use crate::sleep;
use crate::util::{
    coding::{doc_id, id_coding},
    config,
    entity::*,
    error::*,
};
use log::{error, info, warn};
use prost::Message;
use serde_json::Value;
use std::sync::{
    atomic::{AtomicBool, Ordering::SeqCst},
    Arc, RwLock,
};

pub struct Simba {
    conf: Arc<config::Config>,
    _collection: Arc<Collection>,
    pub partition: Arc<Partition>,
    readonly: bool,
    stoped: AtomicBool,
    latch: Latch,
    //engins
    rocksdb: RocksDB,
    tantivy: Tantivy,
}

impl Simba {
    pub fn new(
        conf: Arc<config::Config>,
        readonly: bool,
        collection: Arc<Collection>,
        partition: Arc<Partition>,
    ) -> ASResult<Arc<Simba>> {
        let base = BaseEngine {
            conf: conf.clone(),
            collection: collection.clone(),
            partition: partition.clone(),
            max_sn: RwLock::new(0),
        };

        let rocksdb = RocksDB::new(BaseEngine::new(&base))?;
        let tantivy = Tantivy::new(BaseEngine::new(&base))?;

        //TODO: read all sn . and set value

        let simba = Arc::new(Simba {
            conf: conf.clone(),
            _collection: collection.clone(),
            partition: partition.clone(),
            readonly: readonly,
            stoped: AtomicBool::new(false),
            latch: Latch::new(50000),
            rocksdb: rocksdb,
            tantivy: tantivy,
        });

        let simba_flush = simba.clone();

        tokio::spawn(async move {
            if readonly {
                return;
            }
            info!(
                "to start commit job for partition:{} begin",
                simba_flush.partition.id
            );
            if let Err(e) = simba_flush.flush() {
                panic!(format!(
                    "flush partition:{} has err :{}",
                    simba_flush.partition.id,
                    e.to_string()
                ));
            };
            warn!("parititon:{} stop commit job", simba_flush.partition.id);
        });

        Ok(simba)
    }
    pub fn get(&self, id: &str, sort_key: &str) -> ASResult<Vec<u8>> {
        self.get_by_iid(id_coding(id, sort_key).as_ref())
    }

    fn get_by_iid(&self, iid: &Vec<u8>) -> ASResult<Vec<u8>> {
        match self.rocksdb.db.get(iid) {
            Ok(ov) => match ov {
                Some(v) => Ok(v),
                None => Err(err_code_str_box(NOT_FOUND, "not found!")),
            },
            Err(e) => Err(err_box(format!("get key has err:{}", e.to_string()))),
        }
    }

    //it use 1.estimate of rocksdb  2.index of u64
    pub fn count(&self) -> ASResult<(u64, u64)> {
        let estimate_rocksdb = self
            .rocksdb
            .db
            .property_int_value("rocksdb.estimate-num-keys")?
            .unwrap_or(0);

        let tantivy_count = self.tantivy.count()?;

        Ok((estimate_rocksdb, tantivy_count))
    }

    pub fn search(&self, sdreq: Arc<SearchDocumentRequest>) -> SearchDocumentResponse {
        match self.tantivy.search(sdreq) {
            Ok(r) => r,
            Err(e) => {
                let e = cast_to_err(e);
                SearchDocumentResponse {
                    code: e.0 as i32,
                    total: 0,
                    hits: vec![],
                    info: Some(SearchInfo {
                        error: 1,
                        success: 0,
                        message: format!("search document err:{}", e.1),
                    }),
                }
            }
        }
    }

    pub async fn write(&self, req: WriteDocumentRequest) -> ASResult<()> {
        let (doc, write_type) = (req.doc.unwrap(), WriteType::from_i32(req.write_type));

        match write_type {
            Some(WriteType::Overwrite) => self._overwrite(doc).await,
            Some(WriteType::Create) => self._overwrite(doc).await,
            Some(WriteType::Update) => self._update(doc).await,
            Some(WriteType::Upsert) => self._upsert(doc).await,
            Some(WriteType::Delete) => self._delete(doc).await,
            Some(_) | None => {
                return Err(err_box(format!("can not do the handler:{:?}", write_type)));
            }
        }
    }

    async fn _create(&self, mut doc: Document) -> ASResult<()> {
        let iid = doc_id(&doc);
        doc.version = 1;
        let mut buf1 = Vec::new();
        if let Err(error) = doc.encode(&mut buf1) {
            return Err(error.into());
        }

        let _lock = self.latch.latch_lock(doc.slot);

        if let Err(e) = self.get_by_iid(&iid) {
            let e = cast_to_err(e);
            if e.0 != NOT_FOUND {
                return Err(e);
            }
        } else {
            return Err(err_box(format!("the document:{:?} already exists", iid)));
        }

        self.do_write(&iid, &buf1).await
    }

    async fn _update(&self, mut doc: Document) -> ASResult<()> {
        let (old_version, iid) = (doc.version, doc_id(&doc));

        let _lock = self.latch.latch_lock(doc.slot);
        let old = self.get(doc.id.as_str(), doc.sort_key.as_str())?;
        let old: Document = Message::decode(prost::bytes::Bytes::from(old))?;
        if old_version > 0 && old.version != old_version {
            return Err(err_code_box(
                VERSION_ERR,
                format!(
                    "the document:{} version not right expected:{} found:{}",
                    doc.id, old_version, old.version
                ),
            ));
        }
        merge_doc(&mut doc, old)?;
        doc.version += old_version + 1;
        let mut buf1 = Vec::new();
        if let Err(error) = doc.encode(&mut buf1) {
            return Err(error.into());
        }

        self.do_write(&iid, &buf1).await
    }

    async fn _upsert(&self, mut doc: Document) -> ASResult<()> {
        let iid = doc_id(&doc);
        let _lock = self.latch.latch_lock(doc.slot);
        let old = match self.get_by_iid(iid.as_ref()) {
            Ok(o) => Some(o),
            Err(e) => {
                let e = cast_to_err(e);
                if e.0 == NOT_FOUND {
                    None
                } else {
                    return Err(e);
                }
            }
        };

        if let Some(old) = old {
            let old: Document = Message::decode(prost::bytes::Bytes::from(old))?;
            doc.version = old.version + 1;
            merge_doc(&mut doc, old)?;
        } else {
            doc.version = 1;
        }

        let mut buf1 = Vec::new();
        if let Err(error) = doc.encode(&mut buf1) {
            return Err(error.into());
        }
        self.do_write(&iid, &buf1).await
    }

    async fn _delete(&self, doc: Document) -> ASResult<()> {
        let iid = doc_id(&doc);
        let _lock = self.latch.latch_lock(doc.slot);
        self.do_delete(&iid).await
    }

    async fn _overwrite(&self, mut doc: Document) -> ASResult<()> {
        let iid = doc_id(&doc);
        let mut buf1 = Vec::new();
        doc.version = 1;
        if let Err(error) = doc.encode(&mut buf1) {
            return Err(error.into());
        }
        let _lock = self.latch.latch_lock(doc.slot);
        self.do_write(&iid, &buf1).await
    }

    async fn do_write(&self, key: &Vec<u8>, value: &Vec<u8>) -> ASResult<()> {
        let sn: u64 = 11111; //TODO: get raft sn
        self.rocksdb.write(sn, key, value)?;
        self.tantivy.write(sn, key, value)?;
        Ok(())
    }

    async fn do_delete(&self, key: &Vec<u8>) -> ASResult<()> {
        let sn: u64 = 11111; //TODO: get raft sn
        self.rocksdb.delete(sn, key)?;
        self.tantivy.delete(sn, key)?;
        Ok(())
    }

    pub fn readonly(&self) -> bool {
        return self.readonly;
    }
}

impl Simba {
    fn flush(&self) -> ASResult<()> {
        let flush_time = self.conf.ps.flush_sleep_sec.unwrap_or(3) * 1000;

        let mut pre_db_sn = self.rocksdb.get_sn();
        let mut pre_tantivy_sn = self.rocksdb.get_sn();

        while !self.stoped.load(SeqCst) {
            sleep!(flush_time);

            let mut flag = false;

            if let Some(sn) = self.rocksdb.flush(pre_db_sn) {
                pre_db_sn = sn;
                flag = true;
            }

            if let Some(sn) = self.tantivy.flush(pre_tantivy_sn) {
                pre_tantivy_sn = sn;
                flag = true;
            }

            if flag {
                if let Err(e) = self.rocksdb.write_sn(pre_db_sn, pre_tantivy_sn) {
                    error!("write has err :{:?}", e);
                };
            }
        }
        Ok(())
    }

    pub fn release(&self) {
        self.stoped.store(true, SeqCst);
        self.rocksdb.release();
        self.tantivy.release();
    }
}

fn merge(a: &mut Value, b: Value) {
    match (a, b) {
        (a @ &mut Value::Object(_), Value::Object(b)) => {
            let a = a.as_object_mut().unwrap();
            for (k, v) in b {
                merge(a.entry(k).or_insert(Value::Null), v);
            }
        }
        (a, b) => *a = b,
    }
}

fn merge_doc(new: &mut Document, old: Document) -> ASResult<()> {
    let mut dist: Value = serde_json::from_slice(new.source.as_slice())?;
    let src: Value = serde_json::from_slice(old.source.as_slice())?;
    merge(&mut dist, src);
    new.source = serde_json::to_vec(&dist)?;
    new.version = old.version + 1;
    Ok(())
}
