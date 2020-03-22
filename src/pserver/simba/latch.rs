// Copyright 2020 The Chubao Authors. Licensed under Apache-2.0.

use crate::util::error::*;
use std::sync::Mutex;
pub struct Latch {
    locks: Vec<Mutex<usize>>,
    size: usize,
}

impl Latch {
    pub fn new(size: usize) -> Latch {
        let mut locks = Vec::with_capacity(size);
        for i in 0..size {
            locks.push(Mutex::new(i));
        }
        Latch {
            locks: locks,
            size: size,
        }
    }
    pub fn latch_lock<'a>(&'a self, slot: u32) -> &'a Mutex<usize> {
        &(self.locks[slot as usize % self.size])
    }

    pub fn latch<T>(&self, slot: u32, f: impl FnOnce() -> ASResult<T>) -> ASResult<T> {
        match self.locks[slot as usize % self.size].lock() {
            Ok(_) => f(),
            Err(e) => Err(err_box(format!("get latch lock has err:{}", e.to_string()))),
        }
    }
}
