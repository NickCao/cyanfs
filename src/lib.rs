use fuser::{
    Filesystem, KernelConfig, ReplyAttr, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyStatfs,
    Request, FUSE_ROOT_ID,
};
use serde::{Deserialize, Serialize};

use std::ffi::OsStr;

use std::os::raw::c_int;

use std::time::{Duration, SystemTime};

pub mod block_cache;
pub mod block_dev;

const BLOCK_SIZE: u64 = 512;

#[derive(Serialize, Deserialize, Copy, Clone, PartialEq)]
enum FileKind {
    File,
    Directory,
    Symlink,
}

#[derive(Serialize, Deserialize)]
struct InodeInner {
    pub ino: u64,
    pub size: u64,
    pub kind: FileKind,
    pub perm: u16,
    pub data: Vec<u8>,
    pub blocks: Vec<u64>,
}

#[derive(Serialize, Deserialize)]
struct DirEntry {
    pub ino: u64,
    pub name: String,
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

impl From<InodeInner> for fuser::FileAttr {
    fn from(inner: InodeInner) -> Self {
        fuser::FileAttr {
            ino: inner.ino,
            size: inner.size,
            blocks: inner.blocks.len() as u64,
            crtime: SystemTime::UNIX_EPOCH,
            atime: SystemTime::UNIX_EPOCH,
            mtime: SystemTime::UNIX_EPOCH,
            ctime: SystemTime::UNIX_EPOCH,
            kind: inner.kind.into(),
            perm: inner.perm,
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: BLOCK_SIZE as u32,
            flags: 0,
        }
    }
}

pub struct SFS {
    db: rocksdb::DB,
    dev: block_cache::BlockCache<512, 20>,
}

impl SFS {
    pub fn new(meta: &str, data: &str) -> Self {
        Self {
            db: rocksdb::DB::open_default(meta).unwrap(),
            dev: block_cache::BlockCache::new(data).unwrap(),
        }
    }
}

impl Filesystem for SFS {
    fn init(&mut self, _req: &Request, _config: &mut KernelConfig) -> Result<(), c_int> {
        if self.db.get(FUSE_ROOT_ID.to_ne_bytes()).unwrap().is_none() {
            self.db
                .put(
                    FUSE_ROOT_ID.to_ne_bytes(),
                    bincode::serialize(&InodeInner {
                        ino: FUSE_ROOT_ID,
                        blocks: vec![],
                        data: bincode::serialize::<Vec<DirEntry>>(&vec![]).unwrap(),
                        kind: FileKind::Directory,
                        perm: 0o777,
                        size: 0,
                    })
                    .unwrap(),
                )
                .unwrap();
        }
        Ok(())
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        if let Some(parent) = self.db.get(parent.to_ne_bytes()).unwrap() {
            let parent: InodeInner = bincode::deserialize(&parent).unwrap();
            if parent.kind != FileKind::Directory {
                reply.error(libc::EACCES);
                return;
            }
            let entries: Vec<DirEntry> = bincode::deserialize(parent.data.as_slice()).unwrap();
            match entries.iter().find(|e| OsStr::new(&e.name) == name) {
                Some(e) => {
                    let ino = self.db.get(e.ino.to_ne_bytes()).unwrap().unwrap();
                    let inner: InodeInner = bincode::deserialize(ino.as_slice()).unwrap();
                    reply.entry(&Duration::new(0, 0), &inner.into(), 0);
                }
                None => reply.error(libc::EACCES),
            };
        } else {
            reply.error(libc::EACCES);
        }
    }

    fn getattr(&mut self, _req: &Request, inode: u64, reply: ReplyAttr) {
        let ino = self.db.get(inode.to_ne_bytes()).unwrap().unwrap();
        let inner: InodeInner = bincode::deserialize(ino.as_slice()).unwrap();
        reply.attr(&Duration::new(0, 0), &inner.into());
    }

    fn readdir(
        &mut self,
        _req: &Request,
        inode: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        assert!(offset >= 0);
        let inode = self.db.get(inode.to_ne_bytes()).unwrap().unwrap();
        let inner: InodeInner = bincode::deserialize(&inode).unwrap();
        let entries: Vec<DirEntry> = bincode::deserialize(inner.data.as_slice()).unwrap();
        for (index, entry) in entries.iter().skip(offset as usize).enumerate() {
            let buffer_full: bool = reply.add(
                entry.ino,
                offset + index as i64 + 1,
                entry.kind.into(),
                OsStr::new(&entry.name),
            );
            if buffer_full {
                break;
            }
        }

        reply.ok();
    }

    fn statfs(&mut self, _req: &Request, _ino: u64, reply: ReplyStatfs) {
        reply.statfs(
            u64::MAX,
            u64::MAX,
            u64::MAX,
            0,
            u64::MAX,
            BLOCK_SIZE as u32,
            u32::MAX,
            BLOCK_SIZE as u32,
        );
    }

    fn access(&mut self, _req: &Request, inode: u64, _mask: i32, reply: ReplyEmpty) {
        if self.db.get(inode.to_ne_bytes()).unwrap().is_some() {
            reply.ok();
        } else {
            reply.error(libc::EACCES)
        }
    }
}
