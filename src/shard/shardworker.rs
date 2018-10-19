
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use std::fs::OpenOptions;
use parity_rocksdb::{Options};
use parity_rocksdb::WriteBatch;
use parity_rocksdb::WriteOptions;
use ::shard::shardindex::ShardIndex;
use ::shard::io::IO;
use ::protobuf::*;
use mytypes::types::*;
use ::threadpool::ThreadPool;


pub struct ShardWorker {
    pub post: mpsc::Sender<self::IO>,
    thread: thread::JoinHandle<()>,
    shard_id: i32
//    scan_index : Vec<Pointer>
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
    fn _sorted_key_values(nf: &Node_Fragment) -> ( RepeatedField<Key>, RepeatedField<Value>) {
        let mut indexed_keys: Vec<(usize, &Key)> = Vec::new();
        let mut sorted_keys = ::protobuf::RepeatedField::<Key>::new();
        let mut sorted_values_by_key = ::protobuf::RepeatedField::<Value>::new();

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
    fn sort_key_values(nf: &mut Node_Fragment){

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
                let file_error = format!("Could not open file: {}",&file_path.to_str().expect("valid path"));
                let mut file_out = OpenOptions::new().create(true).write(true).open(&file_path).expect(&file_error);
                let pre_alloc_size = 1024 * 100000;
                file_out.set_len(pre_alloc_size).expect("File size reserved");
                file_out.flush().unwrap();
                let mut file_out = OpenOptions::new().write(true).open(&file_path).expect(&file_error);

                let mut last_file_position = file_out.seek(SeekFrom::Start(0)).expect("Couldn't seek to current position");

                let (file_read_pool_send, file_read_pool_receive) = mpsc::sync_channel(16);
                for i in 1..16 {
                    let fpbc = file_path_buf.clone().into_boxed_path();
                    let path = fpbc.to_str().unwrap();
                    let ppath = path::Path::new(&path);
                    let err_message = file_error.clone();
                    let mut file_in = OpenOptions::new().read(true).append(false).create(false).open(&ppath).expect(&err_message);
                    file_read_pool_send.send(file_in);
                }

                let pool = ThreadPool::new(16);

                loop {
                    let data = receiver.recv_timeout(Duration::from_millis(1));
                    match data {
                        Ok(io) => match io {
                            // todo: throw this Add operation into a future on a current thread pool, so when we block on nodes.recv_timeout we can process from some other IO::Add operation that comes in
                            // because as it is now, all items from the current add operation have to come in and finish being written, before we start to put in any nodes from then next Add operation in line.
                            IO::Add {nodes, callback}  => {

                                // I think we want two memtables. A, and B so that when A is full we can have another thread start writing it out
                                // and in the mean time, start adding data into memtable B.
                                // This means that if a read request comes in that needs to read from memtables
                                // if the data is in the active memtable, then we are ok to read it.
                                // if it is in the memtable that was sent to flush to disk, then we need to wait for that operation to finish,
                                // and then just use a standard file read to access that data. Assuming we aren't yet using DirectIO, that data
                                // would be in the OS page cache.

                                // We likely need a struct to represent this memtable state machine.

                                let memtable_capacity:usize = 1024*16;
                                let buffer_flush_len:usize = 1024*16;
                                let mut memtable : Vec<u8> = Vec::with_capacity(memtable_capacity);

                                let mut index_batch_opts = WriteOptions::new();

                                // todo: figure out what needs to be done so we can disable_wal for more perf.
                                // disable_wal can make writes faster, but we will need to have a
                                // index recovery mechanisim in case we crash, or the process
                                // closes before the index is fully synced.
                                //index_batch_opts.disable_wal(true);

                                let mut index_batch = WriteBatch::new();
                                loop {
                                    // todo: use nodes.try_recv() and if it isn't ready, then take this IO::Add and put it in another channel that we will interleave with the main channel
                                    // we will have to flush the buffer to disk if we put anything in it... unless move the buffer out of this operation and share it across multiple ops.
                                    // Or we could learn how to use the futures crate.
                                    //let xxxx = nodes.try_recv()
                                    let mut node = nodes.recv_timeout(Duration::from_millis(1));
                                    match node {
                                        Ok(ref mut _n) =>{
                                            //println!("Got Nodes");
                                            let buffer_pos = memtable.len() as u64;
                                            let total_pos = buffer_pos + last_file_position;

                                            {
                                                // make sure our fragment has reserved space for other fragment pointers.
                                                // we are using 3 instead of 2 to enable fanning out the fragment loading, vs 2 would be following a linked list.
                                                while _n.fragments.len() > 3 {
                                                    _n.fragments.pop();
                                                }
                                                while _n.fragments.len() < 3 {
                                                    _n.fragments.push(Pointer::new());
                                                }
                                                // todo: check in the nodeid->pointer index to see if we should be setting a pointer.
                                                // and tracking that other fragments need to know about this new one.

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
                                            _n.write_length_delimited_to_vec(&mut memtable).expect("write_to_bytes");

                                            // todo: Would adding to the index in a separate channel speed this up?
                                            // todo: Would a "multi_put" be faster?
                                            // Add this nodeid to the nodeid index
                                            let _key = _n.mut_id().mut_node_id();
                                            let _values = &mut Pointers::new();
                                            {
                                                // todo: Anyway to avoid doing this clone?
                                                let _value = _key.get_node_pointer().clone();
                                                _values.pointers.insert(0, _value);
                                            }
                                            // must clear the node pointer before using it as the Key in the lookup
                                            _key.clear_node_pointer();

                                            index.node_index_merge(&mut index_batch, _key, _values);

                                            if &memtable.len() >= &buffer_flush_len {
                                                // flush the buffer
                                                let _written_size = file_out.write(&memtable).expect("file write");
                                                last_file_position = last_file_position + _written_size as u64;
                                                assert_ne!(0,last_file_position, "We are testing that after wrote data, we are incrementing our last_file_position");
                                                &memtable.clear();
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
                                // todo: can we do this async some how? Look at AIO for linux https://github.com/hmwill/tokio-linux-aio
                                index.db.write(index_batch).unwrap();
                                // flush the buffer
                                let _written_size = file_out.write(&memtable).expect("file write");
                                &memtable.clear();
                                file_out.flush().expect("flush file");
                                callback.send(Ok(())).expect("callback still open");
                                // todo: background work to link fragments
                            },
                            IO::ReadNodeFragments {nodeids, callback} => {
                                // create  a channel to notify us us we read fragments from the pool
                                let (tx,rx) = mpsc::channel();
                                let file_read_pool_send_cp = file_read_pool_send.clone();
                                let mut query =  || {
                                    let movetx = move||{tx};
                                    let tx = movetx();

                                    // /////////////////////////////
                                    // WARNING:: If the client never closes their side of the nodeids channel, then we block here forever.
                                    // /////////////////////////////
                                    for nodeid in nodeids.iter() {

                                        // flush the writer side... if we are going to read from it.
                                        file_out.flush().unwrap();
                                        let mut frag_pointers: Vec<Pointer> = Vec::new();
                                        let mut from_index = false;
                                        if nodeid.has_node_pointer() {
                                            frag_pointers.push(nodeid.get_node_pointer().to_owned());
                                        } else {
                                            from_index = true;
                                            {
                                                let got = &index.node_index_get(&nodeid);
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
                                            let n_frag = frag_pointers.len();
                                            let fpi = frag_pointers.iter();
                                            fpi.for_each(|p| {
                                                let tx = tx.clone();
                                                let offset = p.offset.clone();
                                                let fpbc = file_path_buf.clone().into_boxed_path();

                                                let err_message = file_error.clone();
                                                let my_cb = callback.clone();
                                                let file_in = file_read_pool_receive.recv().unwrap();

                                                // this is super clumsy, but what is going on is we are using a file pool, and a thread pool, and trying to do all the reads as async as possible.
                                                // likely can be done way better. :D But this does seem to be ok fast.

                                                let file_read_pool_send_cp_cp = file_read_pool_send_cp.clone();

                                                pool.execute(move || {
                                                    // todo: most likely its a different file
                                                    // TODO: Do async file IO like: And don't get POSIX AIO and Linux AIO mixed up, different things! seastar linux-aio seems the best so far.
                                                    // In addition, you must use a filesystem that has good support for AIO. Today, and for the foreseeable future, this means XFS.
                                                    // https://www.scylladb.com/2016/02/09/qualifying-filesystems/
                                                    // https://github.com/avikivity/fsqual
                                                    // http://tinyurl.com/msl-scylladb
                                                    // https://github.com/scylladb/seastar/blob/72a729da6cb7f843334d515af2cfd791328f5275/core/linux-aio.cc
                                                    // https://docs.rs/tokio-io/0.1/tokio_io/trait.AsyncRead.html
                                                    // or https://lwn.net/Articles/743714/
                                                    // or https://github.com/sile/linux_aio
                                                    // ref https://www.ibm.com/developerworks/library/l-async/index.html

                                                    let mut f = file_in;
                                                    f.seek(SeekFrom::Start(offset)).map(|offset| {
                                                        let opt = {
                                                            let mut cinp = ::protobuf::stream::CodedInputStream::new(&mut f);
                                                            let fres: ProtobufResult<Node_Fragment> = cinp.read_message::<Node_Fragment>();
                                                            my_cb.send(fres)
                                                        };
                                                        if opt.is_err() {
                                                            error!("Error A: {}", opt.expect_err("only if we have an error"));
                                                        } else {
                                                            let err = opt.map(|()| {
                                                                file_read_pool_send_cp_cp.send(f)
                                                                    .map(|()|{
                                                                        tx.send(1).expect("channel will be there waiting for the pool");
                                                                    })
                                                            });
                                                            if err.is_err(){
                                                                error!("Error B: {}", err.expect_err("only if we have an error"));
                                                            }

                                                        }

                                                    });


                                                });
                                                ()
                                            });


                                            // todo: do we need to send something down callback to tell it we are done?
                                        } else {
                                            // as we load a fragment we either need to follow internal pointers
                                            // or we need to go back to the index.
                                        }
                                    }
                                };
                                query();
                                for done in rx.iter() {
                                    // waiting for them all to be done reading.
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