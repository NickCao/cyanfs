use block_cache::BlockCache;
use fuser::{
    Filesystem, KernelConfig, ReplyAttr, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyStatfs,
    Request, FUSE_ROOT_ID,
};
use rocksdb::DB;
use serde::{Deserialize, Serialize};

use std::ffi::OsStr;

use std::os::raw::c_int;

use std::os::unix::prelude::FileExt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use std::vec;

pub mod block_cache;
pub mod block_dev;

const BLOCK_SIZE: usize = 512;

#[derive(Serialize, Deserialize, Copy, Clone, PartialEq)]
pub enum FileKind {
    File,
    Directory,
    Symlink,
}

pub struct Inode {
    pub inner: InodeInner,
    pub db: Arc<DB>,
    pub dev: Arc<Mutex<BlockCache<BLOCK_SIZE, 20>>>,
}

impl Drop for Inode {
    fn drop(&mut self) {
        self.db
            .put(
                self.inner.ino.to_ne_bytes(),
                &bincode::serialize(&self.inner).unwrap(),
            )
            .unwrap();
    }
}

impl FileExt for Inode {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        let mut data = vec![];
        for block in &self.inner.blocks {
            let mut buf = [0u8; BLOCK_SIZE];
            self.dev.lock()
                .unwrap()
                .read_block(*block, &mut buf)
                .unwrap();
            data.extend_from_slice(&buf);
        }
        let size = std::cmp::min((self.inner.size - offset) as usize, buf.len()) as usize;
        buf[..size].copy_from_slice(&data[offset as usize..offset as usize + size]);
        Ok(size)
    }
    fn write_at(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        Ok(0)
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct InodeInner {
    pub ino: u64,
    pub size: u64,
    pub kind: FileKind,
    pub perm: u16,
    pub blocks: Vec<usize>,
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

impl From<Inode> for fuser::FileAttr {
    fn from(inode: Inode) -> Self {
        fuser::FileAttr {
            ino: inode.inner.ino,
            size: inode.inner.size,
            blocks: inode.inner.blocks.len() as u64,
            crtime: SystemTime::UNIX_EPOCH,
            atime: SystemTime::UNIX_EPOCH,
            mtime: SystemTime::UNIX_EPOCH,
            ctime: SystemTime::UNIX_EPOCH,
            kind: inode.inner.kind.into(),
            perm: inode.inner.perm,
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
    db: Arc<DB>,
    dev: Arc<Mutex<block_cache::BlockCache<BLOCK_SIZE, 20>>>,
    next_inode: usize,
    next_block: usize,
}

impl SFS {
    pub fn new(meta: &str, data: &str) -> Self {
        Self {
            db: Arc::new(rocksdb::DB::open_default(meta).unwrap()),
            dev: Arc::new(Mutex::new(block_cache::BlockCache::new(data).unwrap())),
            next_inode: FUSE_ROOT_ID as usize,
            next_block: 0,
        }
    }
    pub fn read_inode(&self, ino: u64) -> Option<Inode> {
        if let Some(value) = self.db.get(ino.to_ne_bytes()).unwrap() {
            Some(Inode {
                inner: bincode::deserialize(&value).unwrap(),
                db: self.db.clone(),
                dev: self.dev.clone(),
            })
        } else {
            None
        }
    }
    pub fn alloc_block(&mut self) -> usize {
        let block = self.next_block;
        self.next_block += 1;
        block
    }
    pub fn alloc_inode(&mut self) -> usize {
        let inode = self.next_inode;
        self.next_inode += 1;
        inode
    }
    pub fn new_inode(&mut self) -> Inode {
        Inode {
            inner: InodeInner {
                ino: self.alloc_inode() as u64,
                size: 0,
                kind: FileKind::File,
                perm: 0o777,
                blocks: vec![],
            },
            db: self.db.clone(),
            dev: self.dev.clone(),
        }
    }
    pub fn replace_data(&mut self, inode: &mut Inode, data: &[u8]) {
        let mut new_blocks = vec![];
        let chunks = data.chunks(BLOCK_SIZE);
        for chunk in chunks {
            let mut buf = [0u8; BLOCK_SIZE];
            buf[..chunk.len()].copy_from_slice(chunk);
            let block = self.alloc_block();
            self.dev
                .lock().unwrap()
                .write_block(block, &buf)
                .unwrap();
            new_blocks.push(block);
        }
        inode.inner.size = data.len() as u64;
        inode.inner.blocks = new_blocks;
    }
    pub fn read_data(&mut self, inode: &Inode) -> Vec<u8> {
        let mut data = vec![];
        for block in &inode.inner.blocks {
            let mut buf = [0u8; BLOCK_SIZE];
            self.dev.lock()
                .unwrap()
                .read_block(*block, &mut buf)
                .unwrap();
            data.extend_from_slice(&buf);
        }
        data.truncate(inode.inner.size as usize);
        data
    }
}

impl Filesystem for SFS {
    fn init(&mut self, _req: &Request, _config: &mut KernelConfig) -> Result<(), c_int> {
        println!("init");
        if self.read_inode(FUSE_ROOT_ID).is_none() {
            let mut root = self.new_inode();
            root.inner.kind = FileKind::Directory;
            let empty: Vec<DirEntry> = vec![];
            self.replace_data(&mut root, &bincode::serialize(&empty).unwrap())
        }
        let it = self.db.iterator(rocksdb::IteratorMode::Start);
        for (k, v) in it {
            let inode: InodeInner = bincode::deserialize(&v).unwrap();
            let ino = inode.ino as usize;
            if ino >= self.next_inode {
                self.next_inode = ino + 1;
            }
            for b in inode.blocks {
                if b >= self.next_block {
                    self.next_block = b + 1;
                }
            }
        }
        Ok(())
    }
    fn open(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: fuser::ReplyOpen) {
        if let Some(_inode) = self.read_inode(ino) {
            reply.opened(0, 0);
        } else {
            reply.error(libc::ENOENT);
        }
    }
    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        println!("read");
        assert!(offset >= 0);
        if let Some(inode) = self.read_inode(ino) {
            let mut buf = vec![0u8; size as usize];
            let size = inode.read_at(&mut buf, offset as u64).unwrap();
            buf.truncate(size);
            reply.data(&buf);
        } else {
            reply.error(libc::ENOENT);
        }
    }
    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        write_flags: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        println!("write");
        assert!(offset >= 0);
        if let Some(mut inode) = self.read_inode(ino) {
            let block = self.alloc_block();
            inode.inner.blocks.push(block);
            inode.inner.size = BLOCK_SIZE as u64;
            self.dev.lock()
                .unwrap()
                .write_block(block, &[65u8; BLOCK_SIZE])
                .unwrap();
            reply.written(data.len() as u32);
        } else {
            reply.error(libc::ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        println!("getattr");
        if let Some(inode) = self.read_inode(ino) {
            reply.attr(&Duration::new(0, 0), &inode.into());
        } else {
            reply.error(libc::EACCES);
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        println!("readdir");
        assert!(offset >= 0);
        if let Some(inode) = self.read_inode(ino) {
            let data = self.read_data(&inode);
            let entries: Vec<DirEntry> = bincode::deserialize(&data).unwrap();
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
        } else {
            reply.error(libc::EACCES);
        }
    }

    fn statfs(&mut self, _req: &Request, _ino: u64, reply: ReplyStatfs) {
        println!("statfs");
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

    fn access(&mut self, _req: &Request, ino: u64, _mask: i32, reply: ReplyEmpty) {
        println!("access");
        if self.read_inode(ino).is_some() {
            reply.ok();
        } else {
            reply.error(libc::EACCES)
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        println!("setattr");
        reply.attr(&Duration::new(0, 0), &self.read_inode(ino).unwrap().into());
    }
    fn mknod(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        println!("mknod");
        if let Some(mut parent) = self.read_inode(parent) {
            if parent.inner.kind != FileKind::Directory {
                reply.error(libc::EACCES);
                return;
            }
            let mut entries: Vec<DirEntry> =
                bincode::deserialize(&self.read_data(&parent)).unwrap();
            let new_inode = self.new_inode();
            let entry = DirEntry {
                ino: new_inode.inner.ino,
                name: name.to_string_lossy().to_string(),
                kind: FileKind::File,
            };
            entries.push(entry);
            self.replace_data(&mut parent, &bincode::serialize(&entries).unwrap());
            reply.entry(&Duration::new(0, 0), &new_inode.into(), 0);
        } else {
            reply.error(libc::EACCES);
        };
    }
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        print!("lookup");
        if let Some(inode) = self.read_inode(parent) {
            let data = self.read_data(&inode);
            let entries: Vec<DirEntry> = bincode::deserialize(&data).unwrap();
            if let Some(entry) = entries.iter().find(|e| OsStr::new(&e.name) == name) {
                reply.entry(
                    &Duration::new(0, 0),
                    &self.read_inode(entry.ino).unwrap().into(),
                    0,
                );
            } else {
                reply.error(libc::ENOENT);
            }
        } else {
            reply.error(libc::EACCES);
        }
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        println!("mkdir");
        if let Some(mut parent) = self.read_inode(parent) {
            if parent.inner.kind != FileKind::Directory {
                reply.error(libc::EACCES);
                return;
            }
            let mut entries: Vec<DirEntry> =
                bincode::deserialize(&self.read_data(&parent)).unwrap();
            let mut new_inode = self.new_inode();
            new_inode.inner.kind = FileKind::Directory;
            let empty: Vec<DirEntry> = vec![];
            self.replace_data(&mut new_inode, &bincode::serialize(&empty).unwrap());
            let entry = DirEntry {
                ino: new_inode.inner.ino,
                name: name.to_string_lossy().to_string(),
                kind: FileKind::Directory,
            };
            entries.push(entry);
            self.replace_data(&mut parent, &bincode::serialize(&entries).unwrap());
            reply.entry(&Duration::new(0, 0), &new_inode.into(), 0);
        } else {
            reply.error(libc::EACCES);
        };
    }
}
