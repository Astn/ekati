#[macro_use]
extern crate log;
extern crate log4rs;
extern crate protobuf;
extern crate bytes;
extern crate parity_rocksdb;
extern crate threadpool;

mod mytypes;


mod shard;

fn main() {
    log4rs::init_file("config/log4rs.yaml", Default::default()).unwrap();

    info!("booting up");
    warn!("A warning");
    println!("Hello, world!");

}



