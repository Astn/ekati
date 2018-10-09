extern crate log;
extern crate log4rs;
extern crate protobuf;
extern crate bytes;
extern crate parity_rocksdb;

use mytypes;
use shard::shardindex::ShardIndex;
use shard::shardworker::ShardWorker;
use shard::io::IO;

use std::fs::File;
use std::io::prelude::*;
use protobuf::Message;
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
    let start = SystemTime::now();
    let n_fragments = 2000000;
    let shardA_joiner = run_shard_thread(n_fragments/2,1);
    let shardB_joiner= run_shard_thread(n_fragments/2, 2);

    let a_fin = shardA_joiner.join();
    let b_fin = shardB_joiner.join();

    let elapsed =  start.elapsed().unwrap();
    info!("Finished adding {} fragments in {} s {} ms",n_fragments, elapsed.as_secs(), elapsed.subsec_millis());
}


fn run_shard_thread(n_fragments:i32, someShard_id: i32) -> thread::JoinHandle<()> {

    let t = thread::spawn(move ||{

        let someShard = ShardWorker::new(someShard_id, true);

        let starttime = Instant::now();
        let (call_back_initiatior_A, call_back_handler_A) = mpsc::sync_channel::<Result<()>>(1);
        // new scope to cleanup a,b channel
        {
            let (a, b) = mpsc::channel::<mytypes::types::Node>();

            info!("{} - Sending IO:Add",someShard_id);
            someShard.post.send(IO::Add {
                nodes: b,
                callback: call_back_initiatior_A
            }).expect("send failed");


            info!("{} - Starting to send {} fragments",someShard_id, n_fragments);
            let started_at = SystemTime::now();
            for _i in 0..n_fragments {
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
                        nid.set_nodeid(::protobuf::Chars::from(_i.to_string()));
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
                    rf.push({
                        KeyValue::new_with_fields(now,
                                                  Data::new_with_string_data("uses"),
                                                  Data::new_with_string_data("Linux")
                        )
                    });
                    rf.push({
                        KeyValue::new_with_fields(now,
                                                  Data::new_with_string_data("eats"),
                                                  Data::new_with_string_data("pizza")
                        )
                    });
                    rf
                });

                a.send(n).expect("Node send failed");
            }
            let gen_elapsed = started_at.elapsed().unwrap();
            info!("{} - Finished generating and queueing fragments in {} sec, {} ms",someShard_id,gen_elapsed.as_secs(),gen_elapsed.subsec_millis());
        }
        match call_back_handler_A.recv() {
            Ok(status) =>
                match status {
                    Ok(()) => {
                        let elapsed = starttime.elapsed();
                        info!("{} - Finished OK - wrote {} fragments in {}s {}ms",someShard_id,n_fragments,elapsed.as_secs(), elapsed.subsec_millis());
                    },
                    Err(_e) => error!("{} - Finished Err {}",someShard_id, _e.description())
                }
            Err(_e) => error!("{} - Finished Err {}",someShard_id, _e.description())
        }
    });
    t
}