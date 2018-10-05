use std::sync::mpsc;
use std::thread;
use std::collections::BTreeMap;
use mytypes;
use std::time::Duration;
use std::io::Result;
use std::fs::OpenOptions;
use mytypes::types::Pointer;
use parity_rocksdb::{Options, DB, MergeOperands, Writable};

#[cfg(test)]
mod tests;

pub enum IO {
    Add {nodes: mpsc::Receiver<mytypes::types::Node>, callback: mpsc::SyncSender<Result<()>> },
    NoOP
    // add something like Checkpoint {notify: mpsc::Sender<Result<(),Err>>}
}

pub struct ShardWorker {
    pub post: mpsc::Sender<self::IO>,
    thread: thread::JoinHandle<()>,
    shard_id: i32,
    fragment_links_connected: BTreeMap<mytypes::types::Pointer, Vec<mytypes::types::Pointer>>,
    fragment_links_requested: BTreeMap<mytypes::types::Pointer, Vec<mytypes::types::Pointer>>,
    node_index : BTreeMap<mytypes::types::NodeID, Vec<mytypes::types::Pointer>>,
    scan_index : Vec<mytypes::types::Pointer>
}

impl ShardWorker {
    /// Sets the file position and length
    /// Returns the position + length which is the next position
    fn make_pointer(shard_id: i32, last_file_position: u64, size: u64) -> Pointer {
        use ::protobuf::Message;
        let mut ptr = Pointer::new();
        ptr.partition_key = shard_id as u32;
        ptr.filename = shard_id as u32; // todo: this is not correct. idea use 1 byte for level and remaining bytes for file number
        ptr.length = size;
        ptr.offset = last_file_position;
        ptr
    }

    fn node_index_merge(_new_key: &[u8], existing_val: Option<&[u8]>, operands: &mut MergeOperands) -> Vec<u8> {
        use std::cmp::Ordering;
        use protobuf::Message;

        let mut _values = mytypes::types::Pointers::new();
        let mut _result = mytypes::types::Pointers::new();
        // load exising value in if we have one
        for v in existing_val {
            _values.merge_from_bytes(v).unwrap();
        }

        for op in operands {
            let mut _new_val = mytypes::types::Pointers::new();
            _new_val.merge_from_bytes(op).unwrap();
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

    pub fn new(shard_id:i32, create_testing_directory:bool) -> ShardWorker {
        use std::fs;
        use std::env;
        use std::path;
        use std::fs::File;
        use std::io::*;
        use ::protobuf::Message;
        let (a,b) = mpsc::channel::<IO>();
        let test_dir = create_testing_directory.clone();
        let _shard_id = shard_id.clone();

        let sw: ShardWorker = ShardWorker{
            post: a,
            thread: thread::spawn(move ||{
                let dir = match test_dir {
                    true => env::current_dir().unwrap().join(path::Path::new("tmp-data")),
                    false => env::current_dir().unwrap().join(path::Path::new("data"))
                };

                match fs::create_dir(&dir){
                    Ok(_) => Ok(()),
                    Err(ref _e) if _e.kind() == ErrorKind::AlreadyExists => Ok(()),
                    Err(ref _e) => Err(_e)
                }.expect("Create Directory or already exists");


                let node_index_db_path =  dir.join(path::Path::new("node_index"));
                let mut opts = Options::new();
                opts.create_if_missing(true);
                opts.add_merge_operator("node_index_merge",  ShardWorker::node_index_merge);
                let index_nodes = DB::open(&opts, node_index_db_path.to_str().unwrap()).unwrap();



                let file_name = format!("{}.0.a.level",_shard_id);
                let file_path_buf = dir.join(path::Path::new(&file_name));
                let file_path = file_path_buf.as_path();
                let file_error = format!("Could not open file: {}",file_path.to_str().expect("valid path"));
                let mut file = OpenOptions::new().read(true).append(true).create(true).open(&file_path).expect(&file_error);
                let pre_alloc_size = 1024 * 100000;
                file.set_len(pre_alloc_size).expect("File size reserved");
                let mut last_file_position = file.seek(SeekFrom::Current(0)).expect("Couldn't seek to current position");



                let receiver = b;
                loop {
                    let data = receiver.recv_timeout(Duration::from_millis(1));
                    match data {
                        Ok(io) => match io {
                            IO::Add {nodes, callback}  => {
                                let buffer_capacity:usize = 1024*8;
                                let buffer_flush_len:usize = 1024*8;
                                // todo: write to buffer and flush when full
                                let mut buffer : Vec<u8> = Vec::with_capacity(buffer_capacity);
                                loop {
                                    let mut node = nodes.recv_timeout(Duration::from_millis(1));
                                    match node {
                                        Ok(ref mut _n) =>{
                                            //println!("Got Nodes");
                                            let buffer_pos = buffer.len() as u64;
                                            let total_pos = buffer_pos + last_file_position;
                                            {
                                                // make sure our nodeId does not have a pointer in it.
                                                let size_incoming = _n.compute_size();
                                                // scope to hold mut_id
                                                {
                                                    let mut id = _n.mut_id();
                                                    if (&id).has_node_id() {
                                                        id.mut_node_id().set_node_pointer({
                                                            ShardWorker::make_pointer(_shard_id, total_pos, size_incoming as u64)
                                                        });
                                                    } else if (&id).has_global_node_id() {
                                                        // and if we have a global_node_id convert it to local_node_id
                                                        let mut local_id = id.mut_global_node_id().take_nodeid();
                                                        local_id.set_node_pointer({
                                                            ShardWorker::make_pointer(_shard_id, total_pos, size_incoming as u64)
                                                        });
                                                        id.set_node_id(local_id);
                                                    }
                                                }
                                                // todo: Verify that we are creating the correct pointer values
                                                let size_before = _n.compute_size();
                                                // scope for mut_id
                                                {
                                                    let mut id = _n.mut_id();
                                                    id.mut_node_id().set_node_pointer({
                                                        ShardWorker::make_pointer(_shard_id, total_pos, size_before as u64)
                                                    });
                                                }
                                                let size_after = _n.compute_size();
                                                assert_eq!(size_before, size_after, "We are testing to makes sure the size of our fragment didn't change when we set it's pointer");
                                            }
                                            // NOTE: By writing Length Delimited, the offset in our Pointer, points to Length, not the beginning of the data
                                            // So the offset is "offset" by an i32.
                                            &_n.write_length_delimited_to_vec(&mut buffer).expect("write_to_bytes");

                                            // todo: Would adding to the index in a seperate channel speed this up?
                                            // todo: Would a "multi_put" be faster?
                                            // Add this nodeid to the nodeid index
                                            let _key = _n.mut_id().mut_node_id();
                                            let mut _values = mytypes::types::Pointers::new();
                                            {
                                                // todo: Anyway to avoid doing this clone?
                                                let _value = _key.get_node_pointer().clone();
                                                _values.pointers.insert(0, _value);
                                            }
                                            _key.clear_node_pointer();
                                            //index_nodes.put(_key.write_to_bytes().unwrap().as_mut_slice(),_values.write_to_bytes().unwrap().as_mut_slice());
                                            // todo: test this merge instead of a put.
                                            index_nodes.merge(_key.write_to_bytes().unwrap().as_mut_slice(),_values.write_to_bytes().unwrap().as_mut_slice()).unwrap();

                                            if &buffer.len() >= &buffer_flush_len {
                                                let _written_size = file.write(&buffer).expect("file write");
                                                last_file_position = last_file_position + _written_size as u64;
                                                assert_ne!(0,last_file_position, "We are testing that after wrote data, we are incrementing our last_file_position");
                                                &buffer.clear();
                                            }
                                            continue;
                                        },
                                        Err(_e) => {
                                            warn!("Got nodes err: {}", _e);
                                            break;
                                        }
                                    }

                                }
                                let _written_size = file.write(&buffer).expect("file write");
                                &buffer.clear();
                                file.flush().expect("flush file");
                                callback.send(Ok(())).expect("callback still open");
                            },
                            IO::NoOP => debug!("Got NoOp")
                        },
                        Err(_e) => thread::sleep(Duration::from_millis(1))
                    }
                }
            }),
            shard_id,
            fragment_links_connected: BTreeMap::new(),
            fragment_links_requested: BTreeMap::new(),
            node_index: BTreeMap::new(),
            scan_index: Vec::new()
        };
        sw
    }
}