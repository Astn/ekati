use std::sync::mpsc;
use std::thread;
use mytypes;
use std::time::Duration;
use std::fs::OpenOptions;
use mytypes::types::Pointer;
use parity_rocksdb::{Options};
use parity_rocksdb::WriteBatch;
use parity_rocksdb::WriteOptions;
use ::shard::shardindex::ShardIndex;
use ::shard::io::IO;
use ::protobuf::*;
use mytypes::types::Node_Fragment;
use mytypes::types::Key;
use mytypes::types::Value;

pub struct ShardWorker {
    pub post: mpsc::Sender<self::IO>,
    thread: thread::JoinHandle<()>,
    shard_id: i32
//    scan_index : Vec<mytypes::types::Pointer>
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

    /// This is used by sort_key_values internally. Maybe you should use that.
    fn _sorted_key_values(nf: &Node_Fragment) -> ( RepeatedField<mytypes::types::Key>, RepeatedField<Value>) {
        let mut indexed_keys: Vec<(usize, &mytypes::types::Key)> = Vec::new();
        let mut sorted_keys = ::protobuf::RepeatedField::<mytypes::types::Key>::new();
        let mut sorted_values_by_key = ::protobuf::RepeatedField::<mytypes::types::Value>::new();

        let k = nf.get_keys();
        let v = nf.get_values();
        indexed_keys = k.iter().enumerate().collect();

        indexed_keys.sort_unstable_by_key(|t| t.1.get_name());
        // now move the keys and the values by the indexed_keys
        // keys

        for indexed_key in indexed_keys {
            sorted_keys.push(k[indexed_key.0].clone());
            sorted_values_by_key.push(v[indexed_key.0].clone());
        }

        (sorted_keys, sorted_values_by_key)
    }

    /// this replaces the keys and values with sorted ones to enable searching keys, and having
    /// the values ordinal match the keys' ordinal
    fn sort_key_values(nf: &mut mytypes::types::Node_Fragment){

        let (ks,vs) = ShardWorker::_sorted_key_values(&nf);
        nf.set_keys(ks);
        nf.set_values(vs);
    }


    /// Creates and starts up a shard with it's own IO thread.
    pub fn new(shard_id:i32, create_testing_directory:bool) -> ShardWorker {
        use std::fs;
        use std::env;
        use std::path;
        use std::io::*;
        use ::protobuf::Message;

        let (a,receiver) = mpsc::channel::<IO>();
        let test_dir = create_testing_directory.clone();
        let _shard_id = shard_id.clone();

        let sw: ShardWorker = ShardWorker{
            post: a,
            thread: thread::spawn(move ||{
                // todo: allow specifying the directory when creating the shard
                // to allow spreading shards across multiple physical disks
                let dir = env::current_dir().unwrap().join(path::Path::new(&format!("shard-{}",shard_id)));

                match fs::create_dir(&dir){
                    Ok(_) => Ok(()),
                    Err(ref _e) if _e.kind() == ErrorKind::AlreadyExists => Ok(()),
                    Err(ref _e) => Err(_e)
                }.expect("Create Directory or already exists");


                let node_index_db_path =  dir.join(path::Path::new("node_index"));
                // todo: tune it. https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide

                // NodeID -> Pointers
                let mut index = ShardIndex::new(node_index_db_path.to_str().unwrap());

                // todo: level files
                // todo: index files
                let file_name = format!("{}.0.a.level",_shard_id);
                let file_path_buf = dir.join(path::Path::new(&file_name));
                let file_path = file_path_buf.as_path();
                let file_error = format!("Could not open file: {}",file_path.to_str().expect("valid path"));
                let mut file_out = OpenOptions::new().create(true).write(true).open(&file_path).expect(&file_error);
                let pre_alloc_size = 1024 * 100000;
                file_out.set_len(pre_alloc_size).expect("File size reserved");
                file_out.flush();
                let mut file_out = OpenOptions::new().write(true).open(&file_path).expect(&file_error);
                let mut file_in = OpenOptions::new().read(true).append(false).create(false).open(&file_path).expect(&file_error);
                let mut last_file_position = file_out.seek(SeekFrom::Start(0)).expect("Couldn't seek to current position");


                loop {
                    let data = receiver.recv_timeout(Duration::from_millis(1));
                    match data {
                        Ok(io) => match io {
                            // todo: throw this Add operation into a future on a current thread pool, so when we block on nodes.recv_timeout we can process from some other IO::Add operation that comes in
                            // because as it is now, all items from the current add operation have to come in and finish being written, before we start to put in any nodes from then next Add operation in line.
                            IO::Add {nodes, callback}  => {
                                let buffer_capacity:usize = 1024*8;
                                let buffer_flush_len:usize = 1024*8;

                                let mut buffer : Vec<u8> = Vec::with_capacity(buffer_capacity);
                                let mut index_batch_opts = WriteOptions::new();
                                index_batch_opts.disable_wal(true);
                                let mut index_batch = WriteBatch::new();
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

                                            // Must make sure that our keys are sorted for quick lookup later; unless we decide we can do this later.
                                            ShardWorker::sort_key_values(_n);

                                            // NOTE: By writing Length Delimited, the offset in our Pointer, points to Length, not the beginning of the data
                                            // So the offset is "offset" by an i32.
                                            _n.write_length_delimited_to_vec(&mut buffer).expect("write_to_bytes");

                                            // todo: Would adding to the index in a separate channel speed this up?
                                            // todo: Would a "multi_put" be faster?
                                            // Add this nodeid to the nodeid index
                                            let _key = _n.mut_id().mut_node_id();
                                            let _values = &mut mytypes::types::Pointers::new();
                                            {
                                                // todo: Anyway to avoid doing this clone?
                                                let _value = _key.get_node_pointer().clone();
                                                _values.pointers.insert(0, _value);
                                            }
                                            // must clear the node pointer before using it as the Key in the lookup
                                            _key.clear_node_pointer();

                                            index.node_index_merge(&mut index_batch, _key, _values);

                                            if &buffer.len() >= &buffer_flush_len {
                                                // flush the buffer
                                                let _written_size = file_out.write(&buffer).expect("file write");
                                                last_file_position = last_file_position + _written_size as u64;
                                                assert_ne!(0,last_file_position, "We are testing that after wrote data, we are incrementing our last_file_position");
                                                &buffer.clear();
                                            }
                                            continue;
                                        },
                                        Err(_e) => {
                                            // the happy error is - Got nodes err: channel is empty and sending half is closed
                                            // which means we got through all the sent items :)
                                            // anything else is sadness.
                                            warn!("Got nodes err: {}", _e);
                                            break;
                                        }
                                    }

                                }
                                // flush the index_batch
                                // todo: can we do this async some how? We could use some profiling, cause I think, but don't know that this call is a slow blocking call.
                                index.db.write(index_batch);
                                // flush the buffer
                                let _written_size = file_out.write(&buffer).expect("file write");
                                &buffer.clear();
                                file_out.flush().expect("flush file");
                                callback.send(Ok(())).expect("callback still open");
                                // todo: background work to link fragments
                            },
                            IO::ReadNodeFragments {nodeid, callback} => {
                                // flush the writer side... if we are going to read from it.
                                file_out.flush();
                                let mut frag_pointers : Vec<Pointer> = Vec::new();
                                let mut from_index = false;
                                if nodeid.has_node_pointer() {
                                    frag_pointers.push(nodeid.get_node_pointer().to_owned());
                                } else {
                                    from_index = true;
                                    {
                                        let got = index.node_index_get(&nodeid);
                                        got.iter().for_each(|opts| {
                                            opts.iter().for_each(|pts| {
                                                pts.get_pointers().iter().for_each(|p| {
                                                    frag_pointers.push(p.clone());
                                                })
                                            })
                                        });
                                    }
                                }

                                if from_index {
                                    // we should have all the node_fragment pointers we know about
                                    frag_pointers.iter().for_each(|p|{
                                        // todo: most likely its a different file
                                        file_in.seek(SeekFrom::Start(p.offset));
                                        let mut cinp = ::protobuf::stream::CodedInputStream::new(&mut file_in);
                                        let f: ProtobufResult<Node_Fragment> = cinp.read_message::<Node_Fragment>();
                                        callback.send(f);
                                        ()
                                    });
                                    // todo: do we need to send something down callback to tell it we are done?

                                } else {
                                    // as we load a fragment we either need to follow internal pointers
                                    // or we need to go back to the index.
                                }

                            }
                            IO::NoOP => debug!("Got NoOp"),
                            IO::Shutdown => {
                                // todo: if we have a thread pool running maintenance tasks or IO:Add tasks, we need to call shutdown on those.
                                break;
                            }
                        },
                        Err(_e) => thread::sleep(Duration::from_millis(1))
                    }
                }
            }),
            shard_id
        };
        sw
    }
}