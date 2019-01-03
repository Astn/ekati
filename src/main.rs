#[macro_use]
extern crate log;
extern crate log4rs;
extern crate protobuf;
extern crate bytes;
extern crate parity_rocksdb;
extern crate threadpool;
extern crate tokio_linux_aio;
extern crate futures;
extern crate futures_cpupool;
extern crate tokio;
extern crate memmap;
extern crate libc;
extern crate cart_cache;
extern crate aligned_alloc;

mod mytypes;
mod fileio;

mod shard;

fn main() {
    log4rs::init_file("config/log4rs.yaml", Default::default()).unwrap();

    info!("booting up");
    warn!("A warning");
    println!("Hello, world!");

}



