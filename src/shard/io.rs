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
use ::protobuf::ProtobufResult;

pub enum IO {
    Add {nodes: mpsc::Receiver<mytypes::types::Node_Fragment>, callback: mpsc::SyncSender<Result<()>> },
    // Returns all node_fragments for a given Node, if the NodeID has a non-null pointer, then it is
    // used to begin the search, otherwise an index is used to find node_fragment pointers
    ReadNodeFragments {nodeids: mpsc::Receiver<mytypes::types::NodeID>, callback: mpsc::SyncSender<ProtobufResult<mytypes::types::Node_Fragment>>},
    NoOP,
    Shutdown
    // add something like Checkpoint {notify: mpsc::Sender<Result<(),Err>>}
}