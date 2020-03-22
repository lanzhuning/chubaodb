// Copyright 2020 The Chubao Authors. Licensed under Apache-2.0.
use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PTransfer {
    pub collection_id: u32,
    pub partition_id: u32,
    pub to_server: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PCreate {
    pub collection_name: String,
    pub partition_num: u32,
    pub zones: Vec<u32>,
}
