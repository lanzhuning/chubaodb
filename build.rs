// Copyright 2020 The Chubao Authors. Licensed under Apache-2.0.

fn main() {
    tonic_build::compile_protos("proto/pserverpb.proto").unwrap();
}
