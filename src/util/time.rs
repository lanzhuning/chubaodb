// Copyright 2020 The Chubao Authors. Licensed under Apache-2.0.
use chrono::prelude::*;

pub fn current_millis() -> u64 {
    Local::now().timestamp_millis() as u64
}

pub fn timestamp() -> String {
    Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

#[test]
pub fn test_timestamp() {
    assert_ne!(timestamp(), "2014-11-28 12:00:09");
}
