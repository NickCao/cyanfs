use crate::block_cache::BlockCache;
use lru::LruCache;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ops::Range;
use std::os::raw::c_int;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::SystemTime;
use std::vec;

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Debug)]
pub enum FileType {
    RegularFile,
    Directory,
    Symlink,
}

#[derive(Clone)]
pub struct Inode<const BLOCK_SIZE: usize> {
    pub attrs: Attrs<BLOCK_SIZE>,
    pub dirty: bool,
    pub db: Arc<Mutex<cxx::UniquePtr<crate::ffi::KVStore>>>,
    pub dev: Arc<Mutex<BlockCache<BLOCK_SIZE>>>,
}

impl<const BLOCK_SIZE: usize> Inode<BLOCK_SIZE> {
    fn flush(&self) {
        cxx::let_cxx_string!(key = self.attrs.ino.to_le_bytes());
        cxx::let_cxx_string!(value = bincode::serialize(&self.attrs).unwrap());
        if self.attrs.nlink > 0 {
            self.db.lock().unwrap().as_mut().unwrap().put(&key, &value);
        } else {
            self.db.lock().unwrap().as_mut().unwrap().remove(&key);
        }
    }
}

impl<const BLOCK_SIZE: usize> Drop for Inode<BLOCK_SIZE> {
    fn drop(&mut self) {
        if self.dirty {
            self.flush();
        }
    }
}

impl<const BLOCK_SIZE: usize> Attrs<BLOCK_SIZE> {
    pub fn blocks(&self) -> usize {
        self.extents.iter().map(Range::len).sum()
    }
    pub fn read_at(
        &self,
        dev: Arc<Mutex<BlockCache<BLOCK_SIZE>>>,
        buf: &mut [u8],
        offset: u64,
    ) -> std::io::Result<usize> {
        let mut data = vec![];
        let begin = offset as usize / BLOCK_SIZE;
        let end = (offset as usize + buf.len() + (BLOCK_SIZE - 1)) / BLOCK_SIZE;
        for block in self
            .extents
            .iter()
            .flat_map(|r| r.clone())
            .skip(begin)
            .take(end - begin)
        {
            let mut buf = [0u8; BLOCK_SIZE];
            dev.lock().unwrap().read_block(block, &mut buf).unwrap();
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
        for (i, block) in self
            .extents
            .iter()
            .flat_map(|r| r.clone())
            .enumerate()
            .skip(begin)
            .take(end - begin)
        {
            let mut buf = [0u8; BLOCK_SIZE];
            if (i == begin && off != 0) || (i == end && eoff != 0) {
                dev.lock().unwrap().read_block(block, &mut buf).unwrap();
            }
            data.extend_from_slice(&buf);
        }
        data[off..off + buf.len()].copy_from_slice(buf);
        for (i, block) in self
            .extents
            .iter()
            .flat_map(|r| r.clone())
            .skip(begin)
            .take(end - begin)
            .enumerate()
        {
            dev.lock()
                .unwrap()
                .write_block(
                    block,
                    data[i * BLOCK_SIZE..(i + 1) * BLOCK_SIZE]
                        .try_into()
                        .unwrap(),
                )
                .unwrap();
        }
        Ok(buf.len())
    }
    pub fn fsync(&self, dev: Arc<Mutex<BlockCache<BLOCK_SIZE>>>) {
        self.extents
            .iter()
            .flat_map(|r| r.clone())
            .for_each(|block| dev.lock().unwrap().flush_block(block));
    }
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct Attrs<const BLOCK_SIZE: usize> {
    pub ino: u64,
    pub size: u64,
    pub extents: Vec<Range<usize>>,
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
    pub entries: BTreeMap<String, DirEntry>,
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
            blocks: attrs.blocks() as u64,
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
    db: Arc<Mutex<cxx::UniquePtr<crate::ffi::KVStore>>>,
    dev: Arc<Mutex<BlockCache<BLOCK_SIZE>>>,
    cache: LruCache<u64, Inode<BLOCK_SIZE>>,
}

impl<const BLOCK_SIZE: usize> InodeCache<BLOCK_SIZE> {
    pub fn new(
        db: Arc<Mutex<cxx::UniquePtr<crate::ffi::KVStore>>>,
        dev: Arc<Mutex<BlockCache<BLOCK_SIZE>>>,
        capacity: usize,
    ) -> Self {
        Self {
            db,
            dev,
            cache: LruCache::new(capacity),
        }
    }

    pub fn scan(&mut self, mut f: impl FnMut(&Attrs<BLOCK_SIZE>)) -> Result<(), c_int> {
        let ids = self.db.lock().unwrap().list();
        for id in ids.into_iter() {
            let data = self.db.lock().unwrap().get(id);
            if let Ok(attrs) = bincode::deserialize::<Attrs<BLOCK_SIZE>>(data.as_bytes()) {
                f(&attrs);
            } else {
                return Err(libc::EIO);
            }
        }
        Ok(())
    }

    pub fn insert(&mut self, attrs: Attrs<BLOCK_SIZE>) {
        let inode = Inode {
            attrs: attrs.clone(),
            db: self.db.clone(),
            dev: self.dev.clone(),
            dirty: true,
        };
        if attrs.kind == FileType::Directory {
            inode.flush();
        }
        self.cache.put(attrs.ino, inode);
    }

    pub fn read<V>(
        &mut self,
        ino: u64,
        f: impl FnOnce(&Attrs<BLOCK_SIZE>) -> V,
    ) -> Result<V, c_int> {
        if let Some(inode) = self.cache.get(&ino) {
            Ok(f(&inode.attrs))
        } else {
            cxx::let_cxx_string!(key = ino.to_le_bytes());
            let data = self.db.lock().unwrap().get(&key);
            if !data.to_string_lossy().is_empty() {
                if let Ok(attrs) = bincode::deserialize::<Attrs<BLOCK_SIZE>>(data.as_bytes()) {
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
                    Err(libc::EIO)
                }
            } else {
                Err(libc::ENOENT)
            }
        }
    }

    pub fn modify<V>(
        &mut self,
        ino: u64,
        f: impl FnOnce(&mut Attrs<BLOCK_SIZE>) -> V,
    ) -> Result<V, c_int> {
        if let Some(inode) = self.cache.get_mut(&ino) {
            inode.dirty = true;
            let v = Ok(f(&mut inode.attrs));
            if inode.attrs.kind == FileType::Directory {
                inode.flush();
            }
            v
        } else {
            cxx::let_cxx_string!(key = ino.to_le_bytes());
            let data = self.db.lock().unwrap().get(&key);
            if data.to_string_lossy() != "" {
                if let Ok(mut attrs) = bincode::deserialize::<Attrs<BLOCK_SIZE>>(data.as_bytes()) {
                    let v = f(&mut attrs);
                    let inode = Inode {
                        attrs,
                        db: self.db.clone(),
                        dev: self.dev.clone(),
                        dirty: true,
                    };
                    if inode.attrs.kind == FileType::Directory {
                        inode.flush();
                    }
                    self.cache.put(ino, inode);
                    Ok(v)
                } else {
                    Err(libc::EIO)
                }
            } else {
                Err(libc::ENOENT)
            }
        }
    }

    pub fn flush_inode(&mut self, ino: u64) {
        self.cache.pop(&ino);
    }

    pub fn flush(&mut self) {
        self.cache.clear();
        // self.db.lock().unwrap().sync();
    }
}
