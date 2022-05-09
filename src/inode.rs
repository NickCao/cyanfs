use crate::block_cache::BlockCache;
use lru::LruCache;
use no_deadlocks::Mutex;
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::os::raw::c_int;
use std::sync::Arc;
use std::time::SystemTime;
use std::vec;

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Debug)]
pub enum FileType {
    RegularFile,
    Directory,
    Symlink,
}

pub struct Inode<const BLOCK_SIZE: usize> {
    pub attrs: Attrs<BLOCK_SIZE>,
    pub dirty: bool,
    pub db: Arc<DB>,
    pub dev: Arc<Mutex<BlockCache<BLOCK_SIZE>>>,
}

impl<const BLOCK_SIZE: usize> Drop for Inode<BLOCK_SIZE> {
    fn drop(&mut self) {
        if self.dirty {
            self.db
                .put(
                    self.attrs.ino.to_le_bytes(),
                    bincode::serialize(&self.attrs).unwrap(),
                )
                .unwrap();
        }
    }
}

impl<const BLOCK_SIZE: usize> Attrs<BLOCK_SIZE> {
    pub fn read_at(
        &self,
        dev: Arc<Mutex<BlockCache<BLOCK_SIZE>>>,
        buf: &mut [u8],
        offset: u64,
    ) -> std::io::Result<usize> {
        let mut data = vec![];
        let begin = offset as usize / BLOCK_SIZE;
        let end = (offset as usize + buf.len() + (BLOCK_SIZE - 1)) / BLOCK_SIZE;
        for block in self.blocks.iter().skip(begin).take(end - begin) {
            let mut buf = [0u8; BLOCK_SIZE];
            dev.lock().unwrap().read_block(*block, &mut buf).unwrap();
            data.extend_from_slice(&buf);
        }
        let size = std::cmp::min((self.size - offset) as usize, buf.len()) as usize;
        let off = offset as usize % BLOCK_SIZE;
        buf[..size].copy_from_slice(&data[off..off + size]);
        Ok(size)
    }
    pub fn write_at(
        &self,
        dev: Arc<Mutex<BlockCache<BLOCK_SIZE>>>,
        buf: &[u8],
        offset: u64,
    ) -> std::io::Result<usize> {
        let mut data = vec![];
        let begin = offset as usize / BLOCK_SIZE;
        let end = (offset as usize + buf.len() + (BLOCK_SIZE - 1)) / BLOCK_SIZE;
        let off = offset as usize % BLOCK_SIZE;
        let eoff = (offset as usize + buf.len()) % BLOCK_SIZE;
        for (i, block) in self.blocks.iter().enumerate().skip(begin).take(end - begin) {
            let mut buf = [0u8; BLOCK_SIZE];
            if (i == begin && off != 0) || (i == end && eoff != 0) {
                dev.lock().unwrap().read_block(*block, &mut buf).unwrap();
            }
            data.extend_from_slice(&buf);
        }
        data[off..off + buf.len()].copy_from_slice(buf);
        for (i, block) in self.blocks.iter().skip(begin).take(end - begin).enumerate() {
            dev.lock()
                .unwrap()
                .write_block(
                    *block,
                    data[i * BLOCK_SIZE..(i + 1) * BLOCK_SIZE]
                        .try_into()
                        .unwrap(),
                )
                .unwrap();
        }
        Ok(buf.len())
    }
    pub fn fsync(&self, dev: Arc<Mutex<BlockCache<BLOCK_SIZE>>>) {
        self.blocks
            .iter()
            .for_each(|&block| dev.lock().unwrap().flush_block(block));
    }
}

#[derive(Serialize, Deserialize, PartialEq, Clone)]
pub struct Attrs<const BLOCK_SIZE: usize> {
    pub ino: u64,
    pub size: u64,
    pub blocks: Vec<usize>,
    pub atime: SystemTime,
    pub mtime: SystemTime,
    pub ctime: SystemTime,
    pub crtime: SystemTime,
    pub kind: FileType,
    pub perm: u16,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub flags: u32,
    pub entries: HashMap<String, DirEntry>,
    pub link: std::path::PathBuf,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct DirEntry {
    pub ino: u64,
    pub kind: FileType,
}

impl From<FileType> for fuser::FileType {
    fn from(kind: FileType) -> Self {
        match kind {
            FileType::RegularFile => fuser::FileType::RegularFile,
            FileType::Directory => fuser::FileType::Directory,
            FileType::Symlink => fuser::FileType::Symlink,
        }
    }
}

impl<const BLOCK_SIZE: usize> From<&mut Attrs<BLOCK_SIZE>> for fuser::FileAttr {
    fn from(attrs: &mut Attrs<BLOCK_SIZE>) -> Self {
        (&*attrs).into()
    }
}

impl<const BLOCK_SIZE: usize> From<Attrs<BLOCK_SIZE>> for fuser::FileAttr {
    fn from(attrs: Attrs<BLOCK_SIZE>) -> Self {
        attrs.into()
    }
}

impl<const BLOCK_SIZE: usize> From<&Attrs<BLOCK_SIZE>> for fuser::FileAttr {
    fn from(attrs: &Attrs<BLOCK_SIZE>) -> Self {
        fuser::FileAttr {
            ino: attrs.ino,
            size: attrs.size,
            blocks: attrs.blocks.len() as u64,
            crtime: attrs.crtime,
            atime: attrs.atime,
            mtime: attrs.mtime,
            ctime: attrs.ctime,
            kind: attrs.kind.into(),
            perm: attrs.perm,
            nlink: attrs.nlink,
            uid: attrs.uid,
            gid: attrs.gid,
            rdev: attrs.rdev,
            blksize: BLOCK_SIZE as u32,
            flags: attrs.flags,
        }
    }
}

pub struct InodeCache<const BLOCK_SIZE: usize> {
    db: Arc<DB>,
    dev: Arc<Mutex<BlockCache<BLOCK_SIZE>>>,
    cache: LruCache<u64, Inode<BLOCK_SIZE>>,
}

impl<const BLOCK_SIZE: usize> InodeCache<BLOCK_SIZE> {
    pub fn new(db: Arc<DB>, dev: Arc<Mutex<BlockCache<BLOCK_SIZE>>>, capacity: usize) -> Self {
        Self {
            db,
            dev,
            cache: LruCache::new(capacity),
        }
    }

    #[must_use]
    pub fn scan(&mut self, mut f: impl FnMut(&Attrs<BLOCK_SIZE>)) -> Result<(), c_int> {
        let it = self.db.iterator(rocksdb::IteratorMode::Start);
        for (_, data) in it {
            if let Ok(attrs) = bincode::deserialize::<Attrs<BLOCK_SIZE>>(&data) {
                f(&attrs);
            } else {
                log::info!("5");
                return Err(libc::EIO);
            }
        }
        Ok(())
    }

    pub fn insert(&mut self, attrs: Attrs<BLOCK_SIZE>) {
        self.cache.put(
            attrs.ino,
            Inode {
                attrs,
                db: self.db.clone(),
                dev: self.dev.clone(),
                dirty: true,
            },
        );
    }

    #[must_use]
    pub fn read<V>(
        &mut self,
        ino: u64,
        f: impl FnOnce(&Attrs<BLOCK_SIZE>) -> V,
    ) -> Result<V, c_int> {
        if let Some(inode) = self.cache.get(&ino) {
            Ok(f(&inode.attrs))
        } else {
            if let Ok(data) = self.db.get_pinned(ino.to_le_bytes()) {
                if let Some(data) = data {
                    if let Ok(attrs) = bincode::deserialize::<Attrs<BLOCK_SIZE>>(&data) {
                        let v = f(&attrs);
                        self.cache.put(
                            ino,
                            Inode {
                                attrs,
                                db: self.db.clone(),
                                dev: self.dev.clone(),
                                dirty: false,
                            },
                        );
                        Ok(v)
                    } else {
                        log::info!("1");
                        Err(libc::EIO)
                    }
                } else {
                    Err(libc::ENOENT)
                }
            } else {
                log::info!("2");
                Err(libc::EIO)
            }
        }
    }

    #[must_use]
    pub fn modify<V>(
        &mut self,
        ino: u64,
        f: impl FnOnce(&mut Attrs<BLOCK_SIZE>) -> V,
    ) -> Result<V, c_int> {
        if let Some(inode) = self.cache.get_mut(&ino) {
            inode.dirty = true;
            Ok(f(&mut inode.attrs))
        } else {
            if let Ok(data) = self.db.get_pinned(ino.to_le_bytes()) {
                if let Some(data) = data {
                    if let Ok(mut attrs) = bincode::deserialize::<Attrs<BLOCK_SIZE>>(&data) {
                        let v = f(&mut attrs);
                        self.cache.put(
                            ino,
                            Inode {
                                attrs,
                                db: self.db.clone(),
                                dev: self.dev.clone(),
                                dirty: true,
                            },
                        );
                        Ok(v)
                    } else {
                        log::info!("3");
                        Err(libc::EIO)
                    }
                } else {
                    Err(libc::ENOENT)
                }
            } else {
                log::info!("4");
                Err(libc::EIO)
            }
        }
    }

    pub fn flush(&mut self) {
        self.cache.clear()
    }
}
