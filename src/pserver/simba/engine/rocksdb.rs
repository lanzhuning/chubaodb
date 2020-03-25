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
use log::{error, info};
use rocksdb::{FlushOptions, WriteBatch, WriteOptions, DB};
use std::ops::Deref;
use std::path::Path;

const SN_KEY: &'static [u8] = b"____sn";

pub struct RocksDB {
    base: BaseEngine,
    pub db: DB,
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
        let mut option = rocksdb::Options::default();
        option.create_if_missing(true);
        let db = DB::open(&option, db_path.to_str().unwrap())?;
        Ok(RocksDB { base: base, db: db })
    }

    pub fn write(&self, key: &Vec<u8>, value: &Vec<u8>) -> ASResult<()> {
        let mut write_options = WriteOptions::default();
        write_options.disable_wal(true);
        write_options.set_sync(false);
        let mut batch = WriteBatch::default();
        batch.put(key, value)?;
        convert(self.db.write_opt(batch, &write_options))?;
        Ok(())
    }

    pub fn delete(&self, key: &Vec<u8>) -> ASResult<()> {
        let mut write_options = WriteOptions::default();
        write_options.disable_wal(true);
        write_options.set_sync(false);
        let mut batch = WriteBatch::default();
        batch.delete(key)?;
        convert(self.db.write_opt(batch, &write_options))?;
        Ok(())
    }

    pub fn count(&self) -> ASResult<u64> {
        Ok(self
            .db
            .property_int_value("rocksdb.estimate-num-keys")?
            .unwrap_or(0))
    }

    pub fn write_sn(&self, db_sn: u64) -> ASResult<()> {
        let write_options = WriteOptions::default();
        let mut batch = WriteBatch::default();
        batch.put(SN_KEY, &u64_slice(db_sn)[..])?;
        convert(self.db.write_opt(batch, &write_options))?;
        Ok(())
    }
}

impl Engine for RocksDB {
    fn flush(&self) -> ASResult<()> {
        let mut flush_options = FlushOptions::default();
        flush_options.set_wait(false);
        convert(self.db.flush_opt(&flush_options))
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
