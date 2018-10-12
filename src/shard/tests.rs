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
use protobuf::text_format;
use std::thread;
use std::sync::mpsc;
use protobuf::*;
use std::error::Error;
use std::io::Result;
use std::time::Instant;
use mytypes::types::*;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use bytes::Bytes;
use std::string::String;

use std::sync::{Once, ONCE_INIT};
use std::fmt;
use std::fmt::Debug;

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
    let n_fragments = 2;
    let shardA_joiner = run_shard_thread(n_fragments/2,1);
    //let shardB_joiner= run_shard_thread(n_fragments/2, 2);

    let a_fin = shardA_joiner.join();
    //let b_fin = shardB_joiner.join();

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
            let (a, b) = mpsc::channel::<mytypes::types::Node_Fragment>();

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

                let mut n = mytypes::types::Node_Fragment::new();
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

                n.set_keys({
                    let mut ks = ::protobuf::RepeatedField::<Key>::new();
                    ks.push(Key::new_without_attributes(now,"name"));
                    ks.push(Key::new_without_attributes(now,"uses"));
                    ks.push(Key::new_without_attributes(now,"eats"));
                    ks
                });

                n.set_values({
                    let mut vs = ::protobuf::RepeatedField::<Value>::new();
                    vs.push(Value::new_with_data(Data::new_with_string_data("Austin Harris")));
                    vs.push(Value::new_with_data(Data::new_with_string_data("Linux")));
                    vs.push(Value::new_with_data(Data::new_with_string_data("Pizza")));
                    vs
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
        info!("{} - That's not all folks, can we get something out?",someShard_id);

        let mut queryNid = NodeID::new();
        // todo: better way to make chars directly from a string maybe Chars::From<String>(...)
        queryNid.set_graph(::protobuf::Chars::from("default"));
        queryNid.set_nodeid(::protobuf::Chars::from("0".to_string()));

        let (call_back_initiatior_B, call_back_handler_B) = mpsc::sync_channel::<ProtobufResult<Node_Fragment>>(1);

        someShard.post.send(IO::ReadNodeFragments {
            nodeid: queryNid,
            callback: call_back_initiatior_B
        });


        match call_back_handler_B.recv() {

            Ok(status) => {
                match status {
                    Ok(frag) => {
                        ;
                        info!("Woo!!! got a:\n {}",text_format::print_to_string(&frag));
                    },
                    Err(_e) => error!("Sad eyes, got a {}", _e)
                }
            }
            Err(_e) => {
                error!("Sad eyes, got a {}", _e)
            }
        }

    });
    t
}