use std::sync::mpsc;
use std::thread;
use std::collections::BTreeMap;
use mytypes;
use std::time::Duration;
use std::io::Result;
use std::fs::OpenOptions;
use mytypes::types::Pointer;
use parity_rocksdb::{Options, DB, MergeOperands, Writable};
use parity_rocksdb::WriteBatch;
use parity_rocksdb::WriteOptions;
use parity_rocksdb::Column;


pub enum IO {
    Add {nodes: mpsc::Receiver<mytypes::types::Node>, callback: mpsc::SyncSender<Result<()>> },
    NoOP,
    Shutdown
    // add something like Checkpoint {notify: mpsc::Sender<Result<(),Err>>}
}