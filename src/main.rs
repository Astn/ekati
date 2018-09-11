#[macro_use]
extern crate log;
extern crate log4rs;
extern crate protobuf;
extern crate bytes;

mod mytypes;


pub mod shard {
    use std::sync::mpsc;
    use std::thread;
    use std::collections::BTreeMap;
    use mytypes;
    use std::time::Duration;
    use std::error::Error;
    use std::io::Result;
    use std::fs::File;
    use std::fs::OpenOptions;
    use mytypes::types::AddressBlock_oneof_address;
    use mytypes::types::Pointer;
    use mytypes::types::Node;
    use mytypes::types::NodeID;

    pub enum IO {
        Add {nodes: mpsc::Receiver<mytypes::types::Node>, callback: mpsc::SyncSender<Result<()>> },
        NoOP
        // add something like Checkpoint {notify: mpsc::Sender<Result<(),Err>>}
    }
//
//    struct ClusterNode{
//        shards : collections::BTreeMap<i32,ShardWorker>,
//        data_roots : Vec<String>,
//        cluster_config : ClusterConfig
//    }
//
//    impl ClusterNode {
//        fn new(cluster_config: ClusterConfig, data_roots : Vec<String>) -> ClusterNode {
//            let mut cn = ClusterNode{
//                shards: std::collections::BTreeMap::new(),
//                cluster_config,
//                data_roots
//            };
//
//            for i in 0..cn.cluster_config.max_shards {
//                cn.shards.insert(i, ShardWorker::new(i));
//            }
//
//            cn
//        }
//    }
//
//    struct ClusterConfig {
//        max_shards : i32,
//    }
//
//    impl ClusterConfig {
//        fn new(max_shards :i32) -> ClusterConfig {
//            let cc = ClusterConfig{
//                max_shards
//            };
//
//            cc
//        }
//    }

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
        fn make_pointer(shard_id: i32, mut last_file_position: u64, size: u64) -> Pointer {
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
            use std::fs::File;
            use std::io::*;
            use ::protobuf::Message;
            let (a,b) = mpsc::channel::<IO>();
            let test_dir = create_testing_directory.clone();
            let _shard_id = shard_id.clone();
            let sw = ShardWorker{
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

}

fn main() {
    log4rs::init_file("config/log4rs.yaml", Default::default()).unwrap();

    info!("booting up");
    warn!("A warning");
    println!("Hello, world!");

}


#[cfg(test)]
mod tests {
    use super::*;
    use mytypes;

    //use bytes;
    use std::fs::File;
    use std::io::prelude::*;
    use protobuf::Message;
    use std::time::Duration;
    use std::thread;
    use std::sync::mpsc;
    use protobuf::SingularPtrField;
    use protobuf::RepeatedField;
    use std::error::Error;
    use std::io::Result;
    use std::time::Instant;
    use mytypes::types::*;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;
    use bytes::Bytes;
    use std::string::String;

    use std::sync::{Once, ONCE_INIT};

    static INIT: Once = ONCE_INIT;

    /// Setup function that is only run once, even if called multiple times.
    fn setup() {
        INIT.call_once(|| {
            log4rs::init_file("config/log4rs.yaml", Default::default()).unwrap();

            info!("booting up");
        });
    }

    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);

    }

    #[test]
    fn write_buffers_to_disk() {
        setup();
        let mut x = mytypes::types::Pointer::new();
        x.partition_key =101;
        x.offset = 202;
        x.length = 303;
        x.filename = 404;

        let data = x.write_to_bytes().expect("write");
        let mut file = File::create("testfoo.proto.binary").expect("create file");
        file.write(&data).expect("write to file failure");
        file.flush().expect("flush failure");
        for d in &data{
            print!("{}", d)
        }

    }

    #[test]
    fn create_a_shard() {
        setup();
        let shard = shard::ShardWorker::new(1, true);

        shard.post.send(shard::IO::NoOP).expect("Send failed");
        shard.post.send(shard::IO::NoOP).expect("Send failed");
        shard.post.send(shard::IO::NoOP).expect("Send failed");
        shard.post.send(shard::IO::NoOP).expect("Send failed");
        shard.post.send(shard::IO::NoOP).expect("Send failed");
        let starttime = Instant::now();
        let (call_back_initiatior, call_back_handler) = mpsc::sync_channel::<Result<()>>(1);
        // new scope to cleanup a,b channel
        {
            let (a, b) = mpsc::channel::<mytypes::types::Node>();

            shard.post.send(shard::IO::Add {
                nodes: b,
                callback: call_back_initiatior
            }).expect("send failed");

            for i in 0..100000 {
                let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
                    Ok(n) => n.as_secs(),
                    Err(_) => panic!("SystemTime before UNIX EPOCH!")
                };

                let mut n = mytypes::types::Node::new();
                n.set_id({
                    let mut ab = AddressBlock::new();
                    ab.set_node_id({
                        let mut nid = NodeID::new();
                        // todo: better way to make chars directly from a string maybe Chars::From<String>(...)
                        nid.set_graph(::protobuf::Chars::from("default"));
                        nid.set_nodeid(::protobuf::Chars::from("1"));
                        nid
                    });
                    ab});

                n.set_fragments(
                    {
                        let mut rf = ::protobuf::RepeatedField::<Pointer>::new();
                        rf.push(Pointer::new());
                        rf.push(Pointer::new());
                        rf.push(Pointer::new());
                        rf
                    }
                );

                n.set_attributes({
                    let mut rf = ::protobuf::RepeatedField::<KeyValue>::new();
                    rf.push({
                        KeyValue::new_with_fields(now,
                            Data::new_with_string_data("name"),
                            Data::new_with_string_data("Austin Harris")
                           )
                    });
                    rf
                });
//
//            {
//                id: SingularPtrField { value: None, set: false },
//                fragments: RepeatedField { vec: Vec::new(), len: 0 },
//                attributes: RepeatedField { vec: Vec::new(), len: 0 },
//                .. std::default::Default::default()
//            };
                a.send(n).expect("Node send failed");
            }
        }
        match call_back_handler.recv() {
            Ok(status) =>
                match status {
                    Ok(()) => {
                        let elapsed = starttime.elapsed();
                        info!("Finished OK - took: {}s {}ms",elapsed.as_secs(), elapsed.subsec_millis());
                    },
                    Err(_e) => error!("Finished Err {}", _e.description())
                }
            Err(_e) => error!("Finished Err {}", _e.description())
        }
        info!("Finished test");
    }
}

