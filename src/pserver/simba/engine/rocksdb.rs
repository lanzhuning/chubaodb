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
use crate::pserver::simba::engine::engine::{BaseEngine, Engine};
use crate::util::{coding::u64_slice, error::*};
use log::{error, info, warn};
use rocksdb::{FlushOptions, WriteBatch, WriteOptions, DB};
use std::ops::Deref;
use std::path::Path;

pub struct RocksDB {
    base: BaseEngine,
    pub db: DB,
    pub system_db: DB,
}

impl Deref for RocksDB {
    type Target = BaseEngine;
    fn deref<'a>(&'a self) -> &'a BaseEngine {
        &self.base
    }
}

impl RocksDB {
    pub fn new(base: BaseEngine) -> ASResult<RocksDB> {
        let db_path = base.base_path().join(Path::new("db"));
        let sys_path = base.base_path().join(Path::new("sys"));
        let mut option = rocksdb::Options::default();
        option.create_if_missing(true);
        let db = DB::open(&option, db_path.to_str().unwrap())?;
        let system_db = DB::open(&option, sys_path.to_str().unwrap())?;
        Ok(RocksDB {
            base: base,
            db: db,
            system_db: system_db,
        })
    }

    pub fn write(&self, sn: u64, key: &Vec<u8>, value: &Vec<u8>) -> ASResult<()> {
        let mut write_options = WriteOptions::default();
        write_options.disable_wal(true);
        write_options.set_sync(false);
        let mut batch = WriteBatch::default();
        batch.put(key, value)?;
        convert(self.db.write_opt(batch, &write_options))?;
        self.set_sn_if_max(sn);
        Ok(())
    }

    pub fn delete(&self, sn: u64, key: &Vec<u8>) -> ASResult<()> {
        let mut write_options = WriteOptions::default();
        write_options.disable_wal(true);
        write_options.set_sync(false);
        let mut batch = WriteBatch::default();
        batch.delete(key)?;
        convert(self.db.write_opt(batch, &write_options))?;
        self.set_sn_if_max(sn);
        Ok(())
    }

    pub fn write_sn(&self, db_sn: u64, indexer_sn: u64) -> ASResult<()> {
        let write_options = WriteOptions::default();
        let mut batch = WriteBatch::default();
        batch.put(b"db_sn", &u64_slice(db_sn)[..])?;
        batch.put(b"indexer_sn", &u64_slice(indexer_sn)[..])?;
        convert(self.system_db.write_opt(batch, &write_options))?;
        Ok(())
    }
}

impl Engine for RocksDB {
    fn flush(&self, pre_sn: u64) -> Option<u64> {
        let sn = self.get_sn();
        if pre_sn > sn {
            warn!(
                "pre db sn is:{} , db sn is:{}  Impossible！！！！",
                pre_sn, sn
            );
            return Some(pre_sn);
        }
        if pre_sn < sn {
            let mut flush_options = FlushOptions::default();
            flush_options.set_wait(false);
            if let Err(e) = self.db.flush_opt(&flush_options) {
                error!("flush db has err :{:?}", e);
            }
            return Some(sn);
        }
        None
    }

    fn release(&self) {
        info!(
            "the collection:{} , partition:{} to release",
            self.partition.collection_id, self.partition.id
        );
        let mut flush_options = FlushOptions::default();
        flush_options.set_wait(true);
        if let Err(e) = self.db.flush_opt(&flush_options) {
            error!("flush db has err:{:?}", e);
        }
    }
}
