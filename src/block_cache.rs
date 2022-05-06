use crate::block_dev::BlockDevice;
use log::error;
use lru::LruCache;
use std::io::{Read, Result, Write};
use std::path::Path;
use std::sync::Arc;

pub struct Block<const BLOCK_SIZE: usize> {
    buffer: [u8; BLOCK_SIZE],
    block_id: usize,
    dirty: bool,
    dev: Arc<BlockDevice<BLOCK_SIZE>>,
}

impl<const BLOCK_SIZE: usize> Drop for Block<BLOCK_SIZE> {
    fn drop(&mut self) {
        if self.dirty {
            if let Err(err) = self.dev.write_block(self.block_id, &self.buffer) {
                error!(
                    "failed to write back block cache for block id {}, error {}",
                    self.block_id, err
                );
            }
        }
    }
}

pub struct BlockCache<const BLOCK_SIZE: usize, const CACHE_SIZE: usize> {
    dev: Arc<BlockDevice<BLOCK_SIZE>>,
    cache: LruCache<usize, Block<BLOCK_SIZE>>,
}

impl<const BLOCK_SIZE: usize, const CACHE_SIZE: usize> BlockCache<BLOCK_SIZE, CACHE_SIZE> {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(Self {
            dev: Arc::from(BlockDevice::new(path)?),
            cache: LruCache::new(CACHE_SIZE),
        })
    }
    pub fn read_block(&mut self, block_id: usize, buf: &mut [u8; BLOCK_SIZE]) -> Result<()> {
        if let Some(block) = self.cache.get(&block_id) {
            block.buffer.as_slice().read_exact(buf)
        } else {
            self.dev.read_block(block_id, buf)?;
            self.cache.put(
                block_id,
                Block {
                    block_id,
                    buffer: buf.clone(),
                    dev: self.dev.clone(),
                    dirty: false,
                },
            );
            Ok(())
        }
    }
    pub fn write_block(&mut self, block_id: usize, buf: &[u8; BLOCK_SIZE]) -> Result<()> {
        if let Some(block) = self.cache.get_mut(&block_id) {
            block.buffer.as_mut_slice().write_all(buf)
        } else {
            self.cache.put(
                block_id,
                Block {
                    block_id,
                    buffer: buf.clone(),
                    dev: self.dev.clone(),
                    dirty: true,
                },
            );
            Ok(())
        }
    }
}
