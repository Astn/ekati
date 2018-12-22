
use std::result;
use std::sync::mpsc;
use std::thread;
use std::collections::BTreeMap;
use mytypes;
use std::time::Duration;
use std::io::Result;
use std::fs::OpenOptions;
use parity_rocksdb::{Options, DB, MergeOperands, Writable};
use parity_rocksdb::WriteBatch;
use parity_rocksdb::Column;


pub struct ShardIndex {
    pub db: DB,
    pub node_index: [u8;4],
    pub fragments_connected: [u8;4],
    pub fragments_requested: [u8;4]
}
//unsafe impl Send for ShardIndex {}
//unsafe impl Sync for ShardIndex {}



impl ShardIndex {
    pub fn new(path: &str) -> ShardIndex{

        let mut default_opts = Options::new();

        default_opts.create_if_missing(true);
        default_opts.add_merge_operator("node_index_merge", ShardIndex::merge_operator);



        //let d = DB::open_cf(&default_opts, path, &["node_index","fragments_connected", "fragments_requested"], &[node_index_options, fragments_connected_options, fragments_requested_options]).unwrap();
        let d = DB::open(&default_opts, path).unwrap();
        ShardIndex{
            node_index : ['n' as u8,'i' as u8,':' as u8,':' as u8],
            fragments_connected : ['f' as u8,'c' as u8,':' as u8,':' as u8],
            fragments_requested : ['f' as u8,'r' as u8,':' as u8,':' as u8],
            db : d,
        }
    }

    pub fn fragments_connected_merge(&mut self, batch: &mut WriteBatch, pointer: &mut mytypes::types::Pointer, pointers: &mut mytypes::types::Pointers){
        use protobuf::Message;
        use std::io::Write;
        let key = &mut self.fragments_connected.to_vec();
        key.write(&pointer.write_to_bytes().unwrap().as_slice()).unwrap();
        batch.merge(key, pointers.write_to_bytes().unwrap().as_mut_slice()).unwrap();
    }

    pub fn fragments_requested_merge(&mut self, batch: &mut WriteBatch, pointer: &mut mytypes::types::Pointer, pointers: &mut mytypes::types::Pointers){
        use protobuf::Message;
        use std::io::Write;
        let key = &mut self.fragments_requested.to_vec();
        key.write(&pointer.write_to_bytes().unwrap().as_slice()).unwrap();
        batch.merge(key, pointers.write_to_bytes().unwrap().as_mut_slice()).unwrap();
    }

    pub fn node_index_merge(&mut self, batch: &mut WriteBatch, node: &mut mytypes::types::NodeID, pointers: &mut mytypes::types::Pointers){
        use protobuf::Message;
        use std::io::Write;
        let key = &mut self.fragments_requested.to_vec();
        key.write(&node.write_to_bytes().unwrap().as_slice()).unwrap();
        batch.merge(key,pointers.write_to_bytes().unwrap().as_mut_slice()).unwrap();
    }
    /// this only works if node_pointer is not set.
    pub fn node_index_get(&mut self, nodeid: &mytypes::types::NodeID) -> result::Result<Option<mytypes::types::Pointers>,String>  {
        use protobuf::Message;
        use std::io::Write;
        let key = &mut self.fragments_requested.to_vec();
        key.write(&nodeid.write_to_bytes().unwrap().as_slice()).unwrap();
        let found = self.db.get(key);
        let x = found.map(|odbv| {
            odbv.map(|dbv| {
                let mut _values = mytypes::types::Pointers::new();
                _values.merge_from_bytes(dbv.as_ref()).unwrap();
                _values
            })
        });
        x
    }

    /// All the values are currently a mytypes::types::Pointers. If this changes, we will need to check the key for a prefix to select the correct merge operator
    pub fn merge_operator(_new_key: &[u8], existing_val: Option<&[u8]>, operands: &mut MergeOperands) -> Vec<u8> {
        use protobuf::Message;

        let mut _values = mytypes::types::Pointers::new();
        let mut _result = mytypes::types::Pointers::new();
        // load exising value in if we have one
        for v in existing_val {
            _values.merge_from_bytes(v).unwrap();
        }

        for op in operands {
            let mut _new_val = mytypes::types::Pointers::new();
            let merged = _new_val.merge_from_bytes(op);
            if(merged.is_err()){
                error!("Merge_operator failure: {:?}", merged.err().unwrap())
            }
            for p in _new_val.mut_pointers().as_mut_slice() {
                _values.pointers.push(p.clone());
            }
        }
        {
            let mut v = _values.take_pointers();
            {
                v.sort_unstable_by_key(|f| { ((f.filename as u64) << 16) + f.offset });
            }
            let mut vecv = v.into_vec();
            vecv.dedup();
            _result.set_pointers(::protobuf::RepeatedField::from(vecv));
        }
        _result.write_to_bytes().unwrap()
    }
}