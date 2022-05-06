use std::fs::File;
use std::fs::OpenOptions;
use std::io::Result;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::prelude::FileExt;
use std::path::Path;

pub struct BlockDevice<const BLOCK_SIZE: usize> {
    backing_file: File,
}

impl<const BLOCK_SIZE: usize> BlockDevice<BLOCK_SIZE> {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(Self {
            backing_file: OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(libc::O_DIRECT)
                .custom_flags(libc::O_NOATIME)
                .open(path)?,
        })
    }
    pub fn read_block(&self, block_id: usize, buf: &mut [u8; BLOCK_SIZE]) -> Result<()> {
        self.backing_file
            .read_exact_at(buf, (block_id * BLOCK_SIZE) as u64)
    }
    pub fn write_block(&self, block_id: usize, buf: &[u8; BLOCK_SIZE]) -> Result<()> {
        self.backing_file
            .write_all_at(buf, (block_id * BLOCK_SIZE) as u64)
    }
}
