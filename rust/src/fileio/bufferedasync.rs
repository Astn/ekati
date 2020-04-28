use ::protobuf::*;
use mytypes::types::*;
use tokio_linux_aio::AioContext;
use futures_cpupool::CpuPool;
use std::convert;
use std::mem;
use memmap;
use libc::{c_void, mlock};
use libc::{close, open, O_DIRECT, O_RDONLY};
use std::os::unix::io::RawFd;
use std::os::unix::ffi::OsStrExt;
use futures::*;
use futures;
use std::sync;
use tokio_linux_aio::AioReadResultFuture;
use std::slice;
use std::path::Path;
use std::collections::HashMap;
use std::env;
use std::ops::*;
use std::sync::mpsc::SyncSender;
use protobuf::error::ProtobufError::IoError;
use std::sync::mpsc::SendError;
use bytes::{Bytes, BytesMut, Buf, BufMut};
use cart_cache::CartCache;
use std::path::PathBuf;
use std::io::{BufRead, Cursor, Read};

pub struct MemoryHandle {
    block: sync::Arc<sync::RwLock<memmap::MmapMut>>,
}

impl MemoryHandle {
    pub fn new(size: usize) -> MemoryHandle {
        let map = memmap::MmapMut::map_anon(size).unwrap();
        unsafe { mlock(map.as_ref().as_ptr() as *const c_void, map.len()) };
        MemoryHandle {
            block: sync::Arc::new(sync::RwLock::new(map)),
        }
    }
}

impl Clone for MemoryHandle {
    fn clone(&self) -> MemoryHandle {
        MemoryHandle {
            block: self.block.clone(),
        }
    }
}

impl convert::AsRef<[u8]> for MemoryHandle {
    fn as_ref(&self) -> &[u8] {
        unsafe { mem::transmute(&(*self.block.read().unwrap())[..]) }
    }
}

impl convert::AsMut<[u8]> for MemoryHandle {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe { mem::transmute(&mut (*self.block.write().unwrap())[..]) }
    }
}

pub struct OwnedFd {
    pub fd: RawFd,
}

impl OwnedFd {
    pub fn new(path: &PathBuf) -> OwnedFd {
        OwnedFd{
          fd: unsafe {
               open(
              mem::transmute(path.clone().as_os_str().as_bytes().as_ptr()),
              O_DIRECT | O_RDONLY
                ) as RawFd
          }
        }
    }
}

impl Drop for OwnedFd {
    fn drop(&mut self) {
        let result = unsafe { close(self.fd) };
        // assert!(result == 0);
    }
}

pub struct BufferedAsync{
    shard_id: u32,
    aio_context: AioContext,
    files: HashMap<u32,OwnedFd>,
    pool: CpuPool,
    pagecache: sync::Arc<sync::RwLock<CartCache<u64, MemoryHandle>>>
}
impl BufferedAsync{

    pub fn new(pool_size: u32, slots: usize, shard_id: u32) -> BufferedAsync
    {
        let exec = CpuPool::new(pool_size as usize);
        BufferedAsync {
            shard_id,
            files: HashMap::new(),
            aio_context: AioContext::new(&exec, slots).unwrap(),
            pool: exec,
            pagecache: sync::Arc::new(sync::RwLock::new(CartCache::new(1024).unwrap()))
        }
    }

    pub fn read_async(&mut self, fileid : u32, offset : u64, length : u64, callback: SyncSender<Result<Node_Fragment, ProtobufError>>) -> impl Future<Item = Result<(),SendError<Result<Node_Fragment, ProtobufError>>>, Error = Result<(), ProtobufError>>  {

        let my_fd = if self.files.contains_key(&fileid) {
            self.files.get(&fileid).unwrap()
        } else {
            // filename should be = "{filenameInt}.{fileversion}"
            let file_name = format!("{}.0",fileid);
            let dir = env::current_dir().unwrap().join(Path::new(&format!("shard-{}",self.shard_id)));
            let file_path_buf = dir.join(Path::new(&file_name));
            info!("creating FD: {}", file_path_buf.display());
            let owned_fd: OwnedFd = OwnedFd::new(&file_path_buf);
            self.files.insert(fileid, owned_fd);
            self.files.get(&fileid).unwrap()
        };

        // pretty sure this needs to be a block aligned size.
        let inner_offset = offset % 8192;
        let length_with_alignment = inner_offset + length + 1;
        let offset_aligned = (offset / 8192) * 8192 as u64;
        let aligned_size = 8192 + ((length_with_alignment / 8192) * 8192) as u64;
        let pages_needed = aligned_size / 8192;

        // todo: maybe use https://doc.rust-lang.org/std/primitive.u32.html#method.reverse_bits
        let key = offset_aligned.bitor(fileid.shl(16) as u64);
        {
            if pages_needed == 1 && offset_aligned != 0
            {
                let mut reader = self.pagecache.write().unwrap();
                let page = reader.get(&key);
                if page.is_some() {
                    let result_buffer = page.unwrap().to_owned();
                    let page_size = result_buffer.as_ref().len();
                    let mut data_at_offset: &[u8] =
                        unsafe {
                            let data = result_buffer.as_ref().as_ptr().add(inner_offset as usize);
                            let len = aligned_size - (inner_offset);
                            slice::from_raw_parts(data, len as usize)
                        };

                    let mut cinp = ::protobuf::stream::CodedInputStream::new(&mut data_at_offset);
                    let fres: ProtobufResult<Node_Fragment> = cinp.read_message::<Node_Fragment>();
                    if fres.is_err() {
                        error!("hit err: offset: {} aligned_size: {} page_size: {}", offset_aligned, aligned_size, page_size);
                    } else {
                        // info!("hit: offset: {} aligned_size: {} page_size: {}", offset_aligned, aligned_size, page_size);
                    }
                    let early_ret = self.pool.spawn(futures::done(Ok(callback.send(fres))));
                    return early_ret;
                }
            }
        }

        let mut reader = self.pagecache.clone();
        let read_buffer = MemoryHandle::new(aligned_size as usize);

        let x =
            self.aio_context
                .read(my_fd.fd, offset_aligned, read_buffer)
                .map_err(|e|{
                    error!("{:?}",e);
                    Err(IoError(e.error))
                })
                .map(move |result_buffer : MemoryHandle| {
                    // info!("miss: {}",offset_aligned);
                    let blaw = {
                        let mut data_at_offset: &[u8] =
                            unsafe {
                                let data = result_buffer.as_ref().as_ptr().add(inner_offset as usize);
                                let len = aligned_size - (inner_offset);
                                slice::from_raw_parts(data, len as usize)
                            };

                        let mut cinp = ::protobuf::stream::CodedInputStream::new(&mut data_at_offset);
                        let fres: ProtobufResult<Node_Fragment> = cinp.read_message::<Node_Fragment>();

                        callback.send(fres)
                    };
                    let mut rr = reader.write().unwrap();
                    rr.insert(key, result_buffer);


                    blaw
                });

        let y = self.pool.spawn(x);
        y
    }
}