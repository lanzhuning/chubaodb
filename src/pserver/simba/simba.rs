// Copyright 2020 The Chubao Authors. Licensed under Apache-2.0.
use crate::pserver::simba::latch::Latch;
use crate::pserverpb::*;
use crate::util::{
    coding::{doc_id, id_coding},
    config,
    entity::*,
    error::*,
};
use log::{error, info};
use prost::Message;
use rocksdb::{FlushOptions, WriteBatch, WriteOptions, DB};
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, Ordering::SeqCst},
    Arc,
};
use std::thread;

pub struct Simba {
    pub collection_id: u32,
    pub partition: Partition,
    readonly: bool,
    stoped: AtomicBool,
    db: Arc<DB>,
    latch: Latch,
}

impl Simba {
    pub fn new(
        conf: Arc<config::Config>,
        readonly: bool,
        _collection: &Collection,
        partition: &Partition,
    ) -> ASResult<Arc<Simba>> {
        let path = Path::new(&conf.ps.data)
            .join(Path::new(format!("{}", partition.collection_id).as_str()))
            .join(Path::new(format!("{}", partition.id).as_str()))
            .join(Path::new("db"));

        let path_dir = path.to_str().unwrap();

        let mut option = rocksdb::Options::default();
        option.set_wal_dir(path.join("wal").to_str().unwrap());
        option.create_if_missing(true);
        option.set_max_background_flushes(16);
        option.increase_parallelism(16);

        let db = Arc::new(DB::open(&option, path_dir)?);

        let simba = Arc::new(Simba {
            collection_id: partition.collection_id,
            partition: partition.clone(),
            readonly: readonly,
            stoped: AtomicBool::new(false),
            db: db,
            latch: Latch::new(50000),
        });

        Ok(simba)
    }

    pub fn release(&self) {
        self.stoped.store(true, SeqCst);
        let mut flush_options = FlushOptions::default();
        flush_options.set_wait(true);
        if let Err(e) = self.db.flush_opt(&flush_options) {
            error!("flush db has err:{:?}", e);
        }

        while Arc::strong_count(&self.db) > 1 {
            info!(
                "wait release rocksdb collection:{} partition:{} now is :{}",
                self.collection_id,
                self.partition.id,
                Arc::strong_count(&self.db)
            );
            thread::sleep(std::time::Duration::from_millis(300));
        }
    }

    pub fn get(&self, id: &str, sort_key: &str) -> ASResult<Vec<u8>> {
        self.get_by_iid(id_coding(id, sort_key).as_ref())
    }

    fn get_by_iid(&self, iid: &Vec<u8>) -> ASResult<Vec<u8>> {
        match self.db.get(iid) {
            Ok(ov) => match ov {
                Some(v) => Ok(v),
                None => Err(err_code_str_box(NOT_FOUND, "not found!")),
            },
            Err(e) => Err(err_box(format!("get key has err:{}", e.to_string()))),
        }
    }

    //it use estimate
    pub fn count(&self) -> ASResult<u64> {
        match self.db.property_int_value("rocksdb.estimate-num-keys") {
            Ok(ov) => match ov {
                Some(v) => Ok(v),
                None => Ok(0),
            },
            Err(e) => Err(err_box(format!("{}", e.to_string()))),
        }
    }

    pub fn search(&self, _sdreq: Arc<SearchDocumentRequest>) -> SearchDocumentResponse {
        // match self.indexer.search(sdreq) {
        //     Ok(r) => r,
        //     Err(e) => {
        //         let e = cast_to_err(e);
        //         SearchDocumentResponse {
        //             code: e.0 as i32,
        //             total: 0,
        //             hits: vec![],
        //             info: Some(SearchInfo {
        //                 error: 1,
        //                 success: 0,
        //                 message: format!("search document err:{}", e.1),
        //             }),
        //         }
        //     }
        // }
        panic!("see you later")
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

        self.do_write(iid, buf1).await
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

        self.do_write(iid, buf1).await
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
        self.do_write(iid, buf1).await
    }

    async fn _delete(&self, doc: Document) -> ASResult<()> {
        let iid = doc_id(&doc);
        let _lock = self.latch.latch_lock(doc.slot);
        self.do_delete(iid).await
    }

    async fn _overwrite(&self, mut doc: Document) -> ASResult<()> {
        let iid = doc_id(&doc);
        let mut buf1 = Vec::new();
        doc.version = 1;
        if let Err(error) = doc.encode(&mut buf1) {
            return Err(error.into());
        }
        let _lock = self.latch.latch_lock(doc.slot);
        self.do_write(iid, buf1).await
    }

    async fn do_write(&self, key: Vec<u8>, value: Vec<u8>) -> ASResult<()> {
        let mut write_options = WriteOptions::default();
        write_options.disable_wal(true);
        write_options.set_sync(false);
        let mut batch = WriteBatch::default();
        batch.put(key, value)?;
        convert(self.db.write_opt(batch, &write_options))
    }

    async fn do_delete(&self, key: Vec<u8>) -> ASResult<()> {
        let mut write_options = WriteOptions::default();
        write_options.disable_wal(true);
        write_options.set_sync(false);
        let mut batch = WriteBatch::default();
        batch.delete(key)?;
        convert(self.db.write_opt(batch, &write_options))
    }

    pub fn readonly(&self) -> bool {
        return self.readonly;
    }
}

use serde_json::Value;

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
