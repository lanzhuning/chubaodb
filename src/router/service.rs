// Copyright 2020 The Chubao Authors. Licensed under Apache-2.0.

use crate::client::partition_client::PartitionClient;
use crate::client::ps_client::PsClient;
use crate::pserverpb::*;
use crate::util::{config::Config, error::*};
use serde_json::{json, Value};
use std::sync::Arc;

pub struct RouterService {
    ps_client: PsClient,
}

impl RouterService {
    pub async fn new(conf: Arc<Config>) -> ASResult<RouterService> {
        Ok(RouterService {
            ps_client: PsClient::new(conf),
        })
    }

    pub async fn command(&self, bytes: Vec<u8>) -> ASResult<Vec<Value>> {
        let v: Value = serde_json::from_slice(bytes.as_slice()).unwrap();
        let target: Vec<String> = v["target"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();

        let mut result = Vec::with_capacity(target.len());

        for addr in target {
            let rep = PartitionClient::new(addr.clone())
                .command(CommandRequest {
                    body: bytes.clone(),
                })
                .await?;

            let rep: Value = serde_json::from_slice(rep.body.as_slice()).unwrap();
            result.push(json!({
                "addr":addr,
                "result":rep,
            }));
        }
        Ok(result)
    }

    pub async fn write(
        &self,
        collection_name: String,
        id: String,
        sort_key: String,
        version: i64,
        source: Vec<u8>,
        wt: i32,
    ) -> ASResult<GeneralResponse> {
        self.ps_client
            .write(collection_name, id, sort_key, version, source, wt)
            .await
    }

    pub async fn get(
        &self,
        collection_name: String,
        id: String,
        sort_key: String,
    ) -> ASResult<DocumentResponse> {
        self.ps_client.get(collection_name, id, sort_key).await
    }

    pub async fn search(
        &self,
        collection_names: Vec<String>,
        def_fields: Vec<String>,
        query: String,
        size: u32,
    ) -> ASResult<SearchDocumentResponse> {
        self.ps_client
            .search(collection_names[0].as_str(), query, def_fields, size)
            .await
    }

    pub async fn count(&self, collection_name: String) -> ASResult<CountDocumentResponse> {
        self.ps_client.count(collection_name.as_str()).await
    }
}
