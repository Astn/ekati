
extern crate protobuf;
extern crate bytes;

mod mytypes;


pub mod shard {
    use std::sync::mpsc;
    use std::thread;
    use std::collections::BTreeMap;
    use mytypes;
    use std::time::Duration;

    pub enum IO {
        Add {nodes: mpsc::Receiver<mytypes::types::Node> },
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
                                            IO::Add {nodes}  => println!("Got Nodes"),
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
    use super::shard;
    use mytypes;

    //use bytes;
    use std::fs::File;
    use std::io::prelude::*;
    use protobuf::Message;
    use std::time::Duration;
    use std::thread;

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

        thread::sleep(Duration::from_millis(50));
    }

}

