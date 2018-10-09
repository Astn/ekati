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
                let dir = match test_dir {
                    true => env::current_dir().unwrap().join(path::Path::new(&format!("tmp-data-{}",shard_id))),
                    false => env::current_dir().unwrap().join(path::Path::new("data"))
                };

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
                let mut file = OpenOptions::new().read(true).append(true).create(true).open(&file_path).expect(&file_error);
                let pre_alloc_size = 1024 * 100000;
                file.set_len(pre_alloc_size).expect("File size reserved");
                let mut last_file_position = file.seek(SeekFrom::Current(0)).expect("Couldn't seek to current position");


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
                                            // NOTE: By writing Length Delimited, the offset in our Pointer, points to Length, not the beginning of the data
                                            // So the offset is "offset" by an i32.
                                            &_n.write_length_delimited_to_vec(&mut buffer).expect("write_to_bytes");

                                            // todo: Would adding to the index in a seperate channel speed this up?
                                            // todo: Would a "multi_put" be faster?
                                            // Add this nodeid to the nodeid index
                                            let _key = _n.mut_id().mut_node_id();
                                            let _values = &mut mytypes::types::Pointers::new();
                                            {
                                                // todo: Anyway to avoid doing this clone?
                                                let _value = _key.get_node_pointer().clone();
                                                _values.pointers.insert(0, _value);
                                            }
                                            _key.clear_node_pointer();

                                            index.node_index_merge(&mut index_batch, _key, _values);

                                            if &buffer.len() >= &buffer_flush_len {
                                                // flush the buffer
                                                let _written_size = file.write(&buffer).expect("file write");
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
                                let _written_size = file.write(&buffer).expect("file write");
                                &buffer.clear();
                                file.flush().expect("flush file");
                                callback.send(Ok(())).expect("callback still open");
                                // todo: background work to link fragments
                            },
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