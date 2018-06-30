
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
        pub fn new(shard_id:i32) -> ShardWorker {
            let (a,b) = mpsc::channel::<IO>();
            let sw = ShardWorker{
                post: a,
                thread: thread::spawn( ||{
                    let receiver = b;
                    loop {
                        let data = receiver.recv_timeout(Duration::from_millis(1));
                        match data {
                            Ok(io) => match io {
                                            IO::Add {nodes, callback}  => {

                                                loop {
                                                    let node = nodes.recv_timeout(Duration::from_millis(1));
                                                    match node {
                                                        Ok(_n) =>{
                                                            //println!("Got Nodes");
                                                            continue;
                                                        },
                                                        Err(_e) => {
                                                            println!("Got nodes err: {}", _e.description());
                                                            break;
                                                        }
                                                    }

                                                }
                                                callback.send(Ok(()));
                                            },
                                            IO::NoOP => println!("Got NoOp")
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

    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);

    }

    #[test]
    fn write_buffers_to_disk() {
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
        let shard = shard::ShardWorker::new(1);

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
                let n = mytypes::types::Node::new();
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
                        println!("Finished OK - took: {}s {}ms",elapsed.as_secs(), elapsed.subsec_millis());
                    },
                    Err(_e) => println!("Finished Err {}", _e.description())
                }
            Err(_e) => println!("Finished Err {}", _e.description())
        }
        println!("Finished test");
    }

}

