use ::protobuf::*;
use mytypes::types::*;
use ::threadpool::ThreadPool;
use tokio_linux_aio::AioContext;
use futures_cpupool::CpuPool;
use std::convert;
use std::mem;
use memmap;
use libc::{c_long, c_void, mlock};
use libc::{close, open, O_DIRECT, O_RDONLY, O_RDWR};
use std::os::unix::io::RawFd;
use std::os::unix::ffi::OsStrExt;
use futures::Future;
use futures;
use std::sync;
use tokio_linux_aio::AioPollFuture;
use tokio_linux_aio::AioReadResultFuture;
use std::slice;
use std::path::Path;
use std::collections::HashMap;
use std::env;
use futures::Map;
use tokio_linux_aio::AioError;
use std::sync::mpsc::SyncSender;
use futures_cpupool::*;
use protobuf::error::ProtobufError::IoError;
use std::sync::mpsc::SendError;

pub struct MemoryBlock {
    bytes: sync::RwLock<memmap::MmapMut>,
}

impl MemoryBlock {
    pub fn new(size: usize) -> MemoryBlock {
        let map = memmap::MmapMut::map_anon(size).unwrap();
        unsafe { mlock(map.as_ref().as_ptr() as *const c_void, map.len()) };

        MemoryBlock {
            // for real uses, we'll have a buffer pool with locks associated with individual pages
            // simplifying the logic here for test case development
            bytes: sync::RwLock::new(map),
        }
    }
}

pub struct MemoryHandle {
    block: sync::Arc<MemoryBlock>,
}



impl MemoryHandle {
    pub fn new(size: usize) -> MemoryHandle {
        MemoryHandle {
            block: sync::Arc::new(MemoryBlock::new(size)),
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
        unsafe { mem::transmute(&(*self.block.bytes.read().unwrap())[..]) }
    }
}

impl convert::AsMut<[u8]> for MemoryHandle {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe { mem::transmute(&mut (*self.block.bytes.write().unwrap())[..]) }
    }
}

pub struct OwnedFd {
    pub fd: RawFd,
}

impl OwnedFd {
    pub fn new_from_raw_fd(fd: RawFd) -> OwnedFd {
        OwnedFd { fd }
    }
}

impl Drop for OwnedFd {
    fn drop(&mut self) {
        let result = unsafe { close(self.fd) };
        assert!(result == 0);
    }
}

pub struct BufferedAsync{
    shard_id: u32,
    aio_context: AioContext,
    files: HashMap<u32,OwnedFd>,
    pool: CpuPool
}
impl BufferedAsync{

    pub fn new(poolSize: u32, slots: usize, shard_id: u32) -> BufferedAsync
    {
        let exec = CpuPool::new(5);
        BufferedAsync {
            shard_id,
            files: HashMap::new(),
            aio_context: AioContext::new(&exec, slots).unwrap(),
            pool: exec
        }
    }

    pub fn ReadAsync(&mut self, fileid : u32, offset : u64, length : u64, callback: SyncSender<Result<Node_Fragment, ProtobufError>>) -> impl Future<Item = Result<(),SendError<Result<Node_Fragment, ProtobufError>>>, Error = Result<(), ProtobufError>>  {

        let my_fd = if self.files.contains_key(&fileid) {
            self.files.get(&fileid).unwrap()
        } else {
            // filename should be = "{filenameInt}.{fileversion}"
            let file_name = format!("{}.0",fileid);
            let dir = env::current_dir().unwrap().join(Path::new(&format!("shard-{}",self.shard_id)));
            let file_path_buf = dir.join(Path::new(&file_name));

            let owned_fd: OwnedFd = OwnedFd::new_from_raw_fd(unsafe {
                open(
                    mem::transmute(file_path_buf.clone().as_os_str().as_bytes().as_ptr()),
                    O_DIRECT | O_RDONLY,
                )
            });
            self.files.insert(fileid, owned_fd);
            self.files.get(&fileid).unwrap()
        };

        // pretty sure this needs to be a block aligned size.
        let inner_offset = offset % 8192;
        let length_with_alignment = inner_offset + length;
        let offset_aligned = (offset / 8192) * 8192 as u64;
        let aligned_size = 8192 + ((length_with_alignment / 8192) * 8192) as u64;
        let read_buffer = MemoryHandle::new(aligned_size as usize);

        let x =
            self.aio_context
                .read(my_fd.fd, offset_aligned, read_buffer)
                .map_err(|e|{
                    error!("{:?}",e);
                    Err(IoError(e.error))
                })
                .map(move |result_buffer| {
                    //assert!(validate_block(result_buffer.as_ref()));

                    let mut data_at_offset: &[u8] =
                        unsafe {
                            let data = result_buffer.as_ref().as_ptr().add(inner_offset as usize);
                            let len = aligned_size - (inner_offset);
                            slice::from_raw_parts(data, len  as usize)
                        };
                    // Is this expecting it to be length delimited?
                    let mut cinp = ::protobuf::stream::CodedInputStream::new(&mut data_at_offset);
                    let fres: ProtobufResult<Node_Fragment> = cinp.read_message::<Node_Fragment>();
                    {
                        if (fres.is_err()) {
                            error!("offset_inner: {}, offset_aligned:{}, length:{}, bufferSize:{}", inner_offset, offset_aligned, length, aligned_size);
                        }
                    }
                    callback.send(fres)
                });

        let y = self.pool.spawn(x);
        y
    }
}