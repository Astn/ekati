extern crate log;
extern crate log4rs;
extern crate protobuf;
extern crate bytes;
extern crate parity_rocksdb;
extern crate rand;


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
use std::sync::mpsc::TrySendError::Full;
use protobuf::error::ProtobufError::IoError;

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
    //let n_fragments = 1000000;
    let add_data = true;
    let n_fragments = 500000;
    let shard_count = 1 as u32;
    let mut handles = Vec::new();
    for sc in 0..shard_count {
        handles.push(run_shard_thread(n_fragments/shard_count,sc, add_data));
    }
    for sc in 0..shard_count as usize {
        let h = handles.pop();
        h.map(|jh| jh.join());
    }
    let elapsed =  start.elapsed().unwrap();
    info!("Finished shard test of {} fragments in {} s {} ms",n_fragments, elapsed.as_secs(), elapsed.subsec_millis());
    // todo: let rocksdb shutdown gracefully.
}


fn run_shard_thread(n_fragments:u32, some_shard_id: u32, add_data: bool) -> thread::JoinHandle<()> {

    let t = thread::spawn(
        move || {
            let some_shard = ShardWorker::new(some_shard_id, true);

            let starttime = Instant::now();
            let (call_back_initiatior_a, call_back_handler_a) = mpsc::sync_channel::<Result<()>>(1);
            // new scope to cleanup a,b channel

        if add_data
        {
            {
                let (a, b) = mpsc::channel::<mytypes::types::Node_Fragment>();

                info!("thread {} - Sending IO:Add", some_shard_id);
                some_shard.post.send(IO::Add {
                    nodes: b,
                    callback: call_back_initiatior_a
                }).expect("send failed");


                info!("thread {} - Starting to send {} fragments", some_shard_id, n_fragments);
                let started_at = SystemTime::now();
                for _i in 0..n_fragments / 4 {
                    let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
                        Ok(n) => n.as_secs(),
                        Err(_) => panic!("SystemTime before UNIX EPOCH!")
                    };

                    let mut n = mytypes::types::Node_Fragment::new();
                    n.set_id({ AddressBlock::new_with_data("default", _i.to_string().as_str()) });

                    n.add_simple_property(now, "name", "Austin");
                    n.add_simple_property(now, "uses", "Linux");
                    n.add_simple_property(now, "eats", "Pizza");

                    a.send(n).expect("Node send failed");
                }
                for _i in 0..n_fragments / 4 {
                    let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
                        Ok(n) => n.as_secs(),
                        Err(_) => panic!("SystemTime before UNIX EPOCH!")
                    };

                    let mut n = mytypes::types::Node_Fragment::new();
                    n.set_id({ AddressBlock::new_with_data("default", _i.to_string().as_str()) });

                    n.add_simple_property(now, "wants", "async io");
                    n.add_simple_property(now, "but", "only has sync io");
                    n.add_simple_property(now, "need", "better async io in linux");

                    a.send(n).expect("Node send failed");
                }
                for i in 0..n_fragments / 4 {
                    let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
                        Ok(n) => n.as_secs(),
                        Err(_) => panic!("SystemTime before UNIX EPOCH!")
                    };

                    let mut n = mytypes::types::Node_Fragment::new();
                    n.set_id({ AddressBlock::new_with_data("default", i.to_string().as_str()) });
                    let x = 0;
                    let back_ref = i / 25;
                    let forward_ref = (n_fragments / 4) - i;
                    n.add_simple_edge(now, "back", NodeID::new_with_graph_and_id("default", back_ref.to_string().as_str()));
                    n.add_simple_edge(now, "forward", NodeID::new_with_graph_and_id("default", forward_ref.to_string().as_str()));

                    a.send(n).expect("Node send failed");
                }
                for _i in 0..n_fragments / 4 {
                    let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
                        Ok(n) => n.as_secs(),
                        Err(_) => panic!("SystemTime before UNIX EPOCH!")
                    };

                    let mut n = mytypes::types::Node_Fragment::new();
                    n.set_id({ AddressBlock::new_with_data("default", _i.to_string().as_str()) });

                    n.add_simple_property(now, "name", "Austin");

                    a.send(n).expect("Node send failed");
                }
                let gen_elapsed = started_at.elapsed().unwrap();
                info!("thread {} - Finished generating and queueing fragments in {} sec, {} ms", some_shard_id, gen_elapsed.as_secs(), gen_elapsed.subsec_millis());
            }
            match call_back_handler_a.recv() {
                Ok(status) =>
                    match status {
                        Ok(()) => {
                            let elapsed = starttime.elapsed();
                            info!("thread {} - Finished OK - wrote {} fragments in {}s {}ms", some_shard_id, n_fragments, elapsed.as_secs(), elapsed.subsec_millis());
                        },
                        Err(_e) => error!("thread {} - Finished Err {}", some_shard_id, _e.description())
                    }
                Err(_e) => error!("thread {} - Finished Err {}", some_shard_id, _e.description())
            }

            info!("thread {} - That's not all folks, can we get something out?", some_shard_id);
        }


            query_frags(n_fragments, some_shard_id, &some_shard);
            query_frags(n_fragments, some_shard_id, &some_shard);
        });


    t
}

fn query_frags(n_fragments: u32, some_shard_id: u32, some_shard: &ShardWorker) {
    use self::rand::*;
    let rnd_read_cnt = n_fragments / 10;
    info!("thread {} - Testing reading {} random nodes by logical address", some_shard_id, rnd_read_cnt);
    let (call_back_initiatior_b, mut call_back_handler_b) = mpsc::sync_channel::<ProtobufResult<Node_Fragment>>(16);
    let seed = [1,0,0,0, 23,0,0,0, 200,1,0,0, 210,30,0,0,
        0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0];
    let mut rnd = rand::StdRng::from_seed(seed);
    let mut random_ids = Vec::<u32>::new();
    for _i in 0..rnd_read_cnt {
        random_ids.push(_i);
    }
    random_ids.sort_unstable_by_key(|k| {
        rnd.gen_range(0, rnd_read_cnt)
    });
    let started_at_read = SystemTime::now();
// we are doing this to move call_back_initiatior_b into the closure, so it is disposed of when query() finishes executing.
// we do that, because we want call_back_handler_b to receive a shutdown when there are no more messages.
    let mut found_fragments = 0;
    let query = move || {
        let (send_nids, recive_nids) = mpsc::channel::<NodeID>();
        some_shard.post.send(IO::ReadNodeFragments {
            nodeids: recive_nids,
            callback: call_back_initiatior_b.clone()
        }).unwrap();
        for _j in random_ids {
            let mut query_nid = NodeID::new();
            // todo: better way to make chars directly from a string maybe Chars::From<String>(...)
            query_nid.set_graph(::protobuf::Chars::from("default"));
            query_nid.set_nodeid(::protobuf::Chars::from(_j.to_string()));


            match call_back_handler_b.try_recv() {
                Ok(Ok(frag)) => {
                    found_fragments += 1;
                }
                Ok(Err(_e)) => {
                    error!("A -Sad eyes, got a {}", _e);
                }
                Err(_e) => {
                    // expect for this to happen as we may try to receive when there
                    // is nothing to receive
                    //error!("B -Sad eyes, got a {}", _e);
                }
            }
            let sentok = send_nids.send(query_nid).unwrap();
        }
        call_back_handler_b
    };
    call_back_handler_b = query();
    info!("thread - query returned ");
    loop {
        match call_back_handler_b.recv() {
            Ok(Ok(frag)) => {
                found_fragments += 1;
                // info!("got: {}",text_format::print_to_string(&frag));
            }
            Ok(Err(IoError(_e))) => {
                // this is expected when there are no more items to send down the channel
                error!("got: {}", _e);
            }
            Ok(Err(_e)) => {
                // this is expected when there are no more items to send down the channel
                error!("C - Sad eyes, got a {}", _e);
                break;
            }
            Err(_e) => {
                // this is expected when there are no more items to send down the channel
                error!("D - Sad eyes, got a {}", _e);
                break;
            }
        }
    }
    let read_dur = started_at_read.elapsed().unwrap();
    info!("thread {} - Finished OK - read {} fragments in {}s {}ms", some_shard_id, found_fragments, read_dur.as_secs(), read_dur.subsec_millis());
}