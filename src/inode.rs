use crate::block_cache::BlockCache;
use crate::BLOCK_SIZE;
use crate::CACHE_SIZE;
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::os::raw::c_int;
use std::os::unix::prelude::FileExt;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use std::vec;

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Debug)]
pub enum FileType {
    RegularFile,
    Directory,
    Symlink,
}

pub struct Inode {
    pub attrs: Attrs,
    pub db: Arc<DB>,
    pub dev: Arc<Mutex<BlockCache<BLOCK_SIZE, CACHE_SIZE>>>,
}

impl Inode {
    pub fn lookup(
        db: Arc<DB>,
        dev: Arc<Mutex<BlockCache<BLOCK_SIZE, CACHE_SIZE>>>,
        ino: u64,
    ) -> Result<Self, c_int> {
        if let Some(inner) = db.get(ino.to_le_bytes()).map_err(|_| libc::EIO)? {
            Ok(Inode {
                attrs: bincode::deserialize(&inner).map_err(|_| libc::EBADF)?,
                db,
                dev,
            })
        } else {
            Err(libc::ENOENT)
        }
    }
}

impl Drop for Inode {
    fn drop(&mut self) {
        if self.attrs.nlink != 0 {
            self.db
                .put(
                    self.attrs.ino.to_le_bytes(),
                    &bincode::serialize(&self.attrs).unwrap(),
                )
                .unwrap();
        } else {
            self.db.delete(self.attrs.ino.to_le_bytes()).unwrap();
        }
    }
}

impl FileExt for Inode {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        let mut data = vec![];
        let begin = offset as usize / BLOCK_SIZE;
        let end = (offset as usize + buf.len() + (BLOCK_SIZE - 1)) / BLOCK_SIZE;
        for block in self.attrs.blocks.iter().skip(begin).take(end - begin) {
            let mut buf = [0u8; BLOCK_SIZE];
            self.dev
                .lock()
                .unwrap()
                .read_block(*block, &mut buf)
                .unwrap();
            data.extend_from_slice(&buf);
        }
        let size = std::cmp::min((self.attrs.size - offset) as usize, buf.len()) as usize;
        let off = offset as usize % BLOCK_SIZE;
        buf[..size].copy_from_slice(&data[off..off + size]);
        Ok(size)
    }
    fn write_at(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        let mut data = vec![];
        let begin = offset as usize / BLOCK_SIZE;
        let end = (offset as usize + buf.len() + (BLOCK_SIZE - 1)) / BLOCK_SIZE;
        for (i, block) in self
            .attrs
            .blocks
            .iter()
            .enumerate()
            .skip(begin)
            .take(end - begin)
        {
            let mut buf = [0u8; BLOCK_SIZE];
            if i == begin || i == end {
                self.dev
                    .lock()
                    .unwrap()
                    .read_block(*block, &mut buf)
                    .unwrap();
            }
            data.extend_from_slice(&buf);
        }
        let off = offset as usize % BLOCK_SIZE;
        data[off..off + buf.len()].copy_from_slice(buf);
        for (i, block) in self
            .attrs
            .blocks
            .iter()
            .skip(begin)
            .take(end - begin)
            .enumerate()
        {
            self.dev
                .lock()
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
}

#[derive(Serialize, Deserialize)]
pub struct Attrs {
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

#[derive(Serialize, Deserialize, Debug)]
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

impl From<Inode> for fuser::FileAttr {
    fn from(inode: Inode) -> Self {
        fuser::FileAttr {
            ino: inode.attrs.ino,
            size: inode.attrs.size,
            blocks: inode.attrs.blocks.len() as u64,
            crtime: inode.attrs.crtime,
            atime: inode.attrs.atime,
            mtime: inode.attrs.mtime,
            ctime: inode.attrs.ctime,
            kind: inode.attrs.kind.into(),
            perm: inode.attrs.perm,
            nlink: inode.attrs.nlink,
            uid: inode.attrs.uid,
            gid: inode.attrs.gid,
            rdev: inode.attrs.rdev,
            blksize: BLOCK_SIZE as u32,
            flags: inode.attrs.flags,
        }
    }
}
