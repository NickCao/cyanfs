use fuser::{
    Filesystem, KernelConfig, ReplyAttr, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyStatfs,
    Request, FUSE_ROOT_ID,
};
use serde::{Deserialize, Serialize};

use std::ffi::OsStr;

use std::os::raw::c_int;

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

#[derive(Serialize, Deserialize, Clone)]
pub struct Inode {
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
    fn from(inner: Inode) -> Self {
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
    dev: block_cache::BlockCache<BLOCK_SIZE, 20>,
    next_inode: usize,
    next_block: usize,
}

impl SFS {
    pub fn new(meta: &str, data: &str) -> Self {
        Self {
            db: rocksdb::DB::open_default(meta).unwrap(),
            dev: block_cache::BlockCache::new(data).unwrap(),
            next_inode: 0,
            next_block: 0,
        }
    }
    pub fn read_inode(&self, ino: u64) -> Option<Inode> {
        if let Some(value) = self.db.get(ino.to_ne_bytes()).unwrap() {
            Some(bincode::deserialize(&value).unwrap())
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
    pub fn write_inode(&self, inode: &Inode) {
        self.db
            .put(inode.ino.to_ne_bytes(), bincode::serialize(inode).unwrap())
            .unwrap();
    }
    pub fn replace_data(&mut self, inode: &Inode, data: &[u8]) {
        let mut new_blocks = vec![];
        let chunks = data.chunks(BLOCK_SIZE);
        for chunk in chunks {
            let mut buf = [0u8; BLOCK_SIZE];
            buf[..chunk.len()].copy_from_slice(chunk);
            let block = self.alloc_block();
            self.dev.write_block(block, &buf).unwrap();
            new_blocks.push(block);
        }
        let mut new_inode = (*inode).clone();
        new_inode.size = data.len() as u64;
        new_inode.blocks = new_blocks;
        self.write_inode(&new_inode);
    }
    pub fn read_data(&mut self, inode: &Inode) -> Vec<u8> {
        let mut data = vec![];
        for block in &inode.blocks {
            let mut buf = [0u8; BLOCK_SIZE];
            self.dev.read_block(*block, &mut buf).unwrap();
            data.extend_from_slice(&buf);
        }
        data.truncate(inode.size as usize);
        data
    }
}

impl Filesystem for SFS {
    fn init(&mut self, _req: &Request, _config: &mut KernelConfig) -> Result<(), c_int> {
        println!("init");
        if self.read_inode(FUSE_ROOT_ID).is_none() {
            self.write_inode(&Inode {
                ino: FUSE_ROOT_ID,
                blocks: vec![],
                kind: FileKind::Directory,
                perm: 0o777,
                size: 0,
            });
            let root = self.read_inode(FUSE_ROOT_ID).unwrap();
            let empty: Vec<DirEntry> = vec![];
            self.replace_data(&root, &bincode::serialize(&empty).unwrap())
        }
        let it = self.db.iterator(rocksdb::IteratorMode::Start);
        for (k, v) in it {
            let inode: Inode = bincode::deserialize(&v).unwrap();
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
        if let Some(parent) = self.read_inode(parent) {
            if parent.kind != FileKind::Directory {
                reply.error(libc::EACCES);
                return;
            }
            let mut entries: Vec<DirEntry> =
                bincode::deserialize(&self.read_data(&parent)).unwrap();
            let new_inode = Inode {
                ino: self.alloc_inode() as u64,
                kind: FileKind::File,
                perm: mode as u16,
                blocks: vec![],
                size: 0,
            };
            self.write_inode(&new_inode);
            let entry = DirEntry {
                ino: new_inode.ino,
                name: name.to_string_lossy().to_string(),
                kind: FileKind::File,
            };
            entries.push(entry);
            self.replace_data(&parent, &bincode::serialize(&entries).unwrap());
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
        if let Some(parent) = self.read_inode(parent) {
            if parent.kind != FileKind::Directory {
                reply.error(libc::EACCES);
                return;
            }
            let mut entries: Vec<DirEntry> =
                bincode::deserialize(&self.read_data(&parent)).unwrap();
            let new_inode = Inode {
                ino: self.alloc_inode() as u64,
                kind: FileKind::Directory,
                perm: mode as u16,
                blocks: vec![],
                size: 0,
            };
            let empty: Vec<DirEntry> = vec![];
            self.write_inode(&new_inode);
            self.replace_data(&new_inode, &bincode::serialize(&empty).unwrap());
            let entry = DirEntry {
                ino: new_inode.ino,
                name: name.to_string_lossy().to_string(),
                kind: FileKind::Directory,
            };
            entries.push(entry);
            self.replace_data(&parent, &bincode::serialize(&entries).unwrap());
            reply.entry(&Duration::new(0, 0), &new_inode.into(), 0);
        } else {
            reply.error(libc::EACCES);
        };
    }
}
