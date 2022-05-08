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

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq)]
pub enum FileKind {
    File,
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
        self.db
            .put(
                self.attrs.ino.to_le_bytes(),
                &bincode::serialize(&self.attrs).unwrap(),
            )
            .unwrap();
    }
}

impl FileExt for Inode {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        let mut data = vec![];
        for block in &self.attrs.blocks {
            let mut buf = [0u8; BLOCK_SIZE];
            self.dev
                .lock()
                .unwrap()
                .read_block(*block, &mut buf)
                .unwrap();
            data.extend_from_slice(&buf);
        }
        let size = std::cmp::min((self.attrs.size - offset) as usize, buf.len()) as usize;
        buf[..size].copy_from_slice(&data[offset as usize..offset as usize + size]);
        Ok(size)
    }
    fn write_at(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        let mut data = vec![];
        for block in &self.attrs.blocks {
            let mut buf = [0u8; BLOCK_SIZE];
            self.dev
                .lock()
                .unwrap()
                .read_block(*block, &mut buf)
                .unwrap();
            data.extend_from_slice(&buf);
        }
        data[offset as usize..offset as usize + buf.len()].copy_from_slice(buf);
        for (i, block) in self.attrs.blocks.iter().enumerate() {
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
    pub kind: FileKind,
    pub perm: u16,
    pub nlinks: u64,
    pub entries: HashMap<String, DirEntry>,
    pub link: std::path::PathBuf,
    pub blocks: Vec<usize>,
}

#[derive(Serialize, Deserialize)]
pub struct DirEntry {
    pub ino: u64,
    pub kind: FileKind,
}

impl From<FileKind> for fuser::FileType {
    fn from(kind: FileKind) -> Self {
        match kind {
            FileKind::File => fuser::FileType::RegularFile,
            FileKind::Directory => fuser::FileType::Directory,
            FileKind::Symlink => fuser::FileType::Symlink,
        }
    }
}

impl From<Inode> for fuser::FileAttr {
    fn from(inode: Inode) -> Self {
        fuser::FileAttr {
            ino: inode.attrs.ino,
            size: inode.attrs.size,
            blocks: inode.attrs.blocks.len() as u64,
            crtime: SystemTime::UNIX_EPOCH,
            atime: SystemTime::UNIX_EPOCH,
            mtime: SystemTime::UNIX_EPOCH,
            ctime: SystemTime::UNIX_EPOCH,
            kind: inode.attrs.kind.into(),
            perm: inode.attrs.perm,
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: BLOCK_SIZE as u32,
            flags: 0,
        }
    }
}
