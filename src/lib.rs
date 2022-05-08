use block_cache::BlockCache;
use fuser::{
    Filesystem, KernelConfig, ReplyAttr, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyStatfs,
    Request, FUSE_ROOT_ID,
};
use rocksdb::DB;
use serde::{Deserialize, Serialize};

use std::collections::HashMap;
use std::ffi::OsStr;

use std::os::raw::c_int;

use std::os::unix::prelude::{FileExt, OsStrExt};
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
            self.dev
                .lock()
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
        let mut data = vec![];
        for block in &self.inner.blocks {
            let mut buf = [0u8; BLOCK_SIZE];
            self.dev
                .lock()
                .unwrap()
                .read_block(*block, &mut buf)
                .unwrap();
            data.extend_from_slice(&buf);
        }
        data[offset as usize..offset as usize + buf.len()].copy_from_slice(buf);
        for (i, block) in self.inner.blocks.iter().enumerate() {
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
pub struct InodeInner {
    pub ino: u64,
    pub size: u64,
    pub kind: FileKind,
    pub perm: u16,
    pub nlinks: u64,
    pub xattrs: HashMap<String, Vec<u8>>,
    pub entries: HashMap<String, DirEntry>,
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
        self.db.get(ino.to_ne_bytes()).unwrap().map(|value| Inode {
            inner: bincode::deserialize(&value).unwrap(),
            db: self.db.clone(),
            dev: self.dev.clone(),
        })
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
                nlinks: 1,
                entries: HashMap::new(),
                xattrs: HashMap::new(),
                blocks: vec![],
            },
            db: self.db.clone(),
            dev: self.dev.clone(),
        }
    }
    pub fn remove_dirent(&mut self, parent: u64, name: &OsStr) -> Option<()> {
        if let Some(mut inode) = self.read_inode(parent) {
            if let Some(_entry) = inode.inner.entries.remove(name.to_str().unwrap()) {
                Some(())
            } else {
                None
            }
        } else {
            None
        }
    }
    pub fn lookup_dirent(&mut self, parent: u64, name: &OsStr) -> Option<Inode> {
        if let Some(inode) = self.read_inode(parent) {
            inode
                .inner
                .entries
                .get(name.to_str().unwrap())
                .map(|entry| self.read_inode(entry.ino).unwrap())
        } else {
            None
        }
    }
    pub fn replace_data(&mut self, inode: &mut Inode, data: &[u8]) {
        let mut new_blocks = vec![];
        let chunks = data.chunks(BLOCK_SIZE);
        for chunk in chunks {
            let mut buf = [0u8; BLOCK_SIZE];
            buf[..chunk.len()].copy_from_slice(chunk);
            let block = self.alloc_block();
            self.dev.lock().unwrap().write_block(block, &buf).unwrap();
            new_blocks.push(block);
        }
        inode.inner.size = data.len() as u64;
        inode.inner.blocks = new_blocks;
    }
    pub fn read_data(&mut self, inode: &Inode) -> Vec<u8> {
        let mut data = vec![];
        for block in &inode.inner.blocks {
            let mut buf = [0u8; BLOCK_SIZE];
            self.dev
                .lock()
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
        if self.read_inode(FUSE_ROOT_ID).is_none() {
            let mut root = self.new_inode();
            root.inner.kind = FileKind::Directory;
        }
        let it = self.db.iterator(rocksdb::IteratorMode::Start);
        for (_k, v) in it {
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
    fn destroy(&mut self) {
        self.db.flush().unwrap();
        self.dev.lock().unwrap().flush();
    }
    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
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
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        println!("write");
        assert!(offset >= 0);
        if let Some(mut inode) = self.read_inode(ino) {
            let new_size = offset as usize + data.len();
            if new_size > inode.inner.size as usize {
                inode.inner.size = new_size as u64;
            }
            let block_cnt = (new_size + (BLOCK_SIZE - 1)) / BLOCK_SIZE;
            while block_cnt > inode.inner.blocks.len() {
                inode.inner.blocks.push(self.alloc_block());
            }
            let size = inode.write_at(data, offset as u64).unwrap();
            reply.written(size as u32);
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
            for (index, (name, entry)) in
                inode.inner.entries.iter().skip(offset as usize).enumerate()
            {
                let buffer_full: bool = reply.add(
                    entry.ino,
                    offset + index as i64 + 1,
                    entry.kind.into(),
                    OsStr::new(&name),
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
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        println!("setattr");
        if let Some(mut inode) = self.read_inode(ino) {
            if let Some(size) = size {
                inode.inner.size = size;
            }
            if let Some(mode) = mode {
                inode.inner.perm = mode as u16;
            }
            reply.attr(&Duration::new(0, 0), &inode.into());
        } else {
            reply.error(libc::ENOENT);
        }
    }
    fn mknod(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        println!("mknod");
        if let Some(mut parent) = self.read_inode(parent) {
            if parent.inner.kind != FileKind::Directory {
                reply.error(libc::EACCES);
                return;
            }
            let new_inode = self.new_inode();
            if parent.inner.entries.contains_key(name.to_str().unwrap()) {
                reply.error(libc::EEXIST);
                return;
            }
            parent.inner.entries.insert(
                name.to_str().unwrap().to_string(),
                DirEntry {
                    ino: new_inode.inner.ino,
                    kind: FileKind::File,
                },
            );
            reply.entry(&Duration::new(0, 0), &new_inode.into(), 0);
        } else {
            reply.error(libc::EACCES);
        };
    }
    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        print!("unlink");
        if let Some(mut inode) = self.read_inode(parent) {
            if inode.inner.entries.remove(name.to_str().unwrap()).is_some() {
                reply.ok();
            } else {
                reply.error(libc::ENOENT);
            }
        } else {
            reply.error(libc::EBADF);
        }
    }
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        print!("lookup");
        if let Some(entry) = self.lookup_dirent(parent, name) {
            reply.entry(&Duration::new(0, 0), &entry.into(), 0);
        } else {
            reply.error(libc::ENOENT);
        }
    }
    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        println!("mkdir");
        if let Some(mut parent) = self.read_inode(parent) {
            if parent.inner.kind != FileKind::Directory {
                reply.error(libc::EACCES);
                return;
            }
            let mut new_inode = self.new_inode();
            new_inode.inner.kind = FileKind::Directory;
            if parent.inner.entries.contains_key(name.to_str().unwrap()) {
                reply.error(libc::EEXIST);
                return;
            }
            parent.inner.entries.insert(
                name.to_str().unwrap().to_string(),
                DirEntry {
                    ino: new_inode.inner.ino,
                    kind: FileKind::Directory,
                },
            );
            reply.entry(&Duration::new(0, 0), &new_inode.into(), 0);
        } else {
            reply.error(libc::EACCES);
        };
    }
    fn link(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        if let Some(mut inode) = self.read_inode(ino) {
            if let Some(mut parent) = self.read_inode(newparent) {
                parent.inner.entries.insert(
                    newname.to_str().unwrap().to_string(),
                    DirEntry {
                        ino: inode.inner.ino,
                        kind: inode.inner.kind,
                    },
                );
                inode.inner.nlinks += 1;
                reply.entry(&Duration::new(0, 0), &inode.into(), 0);
            }
        } else {
            reply.error(libc::EBADF);
        }
    }
    fn rmdir(&mut self, _req: &Request<'_>, _parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(libc::ENOSYS);
    }
    fn flush(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        reply.error(libc::ENOSYS);
    }
    fn fsync(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        reply.error(libc::ENOSYS);
    }
    fn getlk(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        _start: u64,
        _end: u64,
        _typ: i32,
        _pid: u32,
        reply: fuser::ReplyLock,
    ) {
        reply.error(libc::ENOSYS);
    }
    fn setlk(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        _start: u64,
        _end: u64,
        _typ: i32,
        _pid: u32,
        _sleep: bool,
        reply: ReplyEmpty,
    ) {
        reply.error(libc::ENOSYS);
    }
    fn ioctl(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: u32,
        _cmd: u32,
        _in_data: &[u8],
        _out_size: u32,
        reply: fuser::ReplyIoctl,
    ) {
        reply.error(libc::ENOSYS);
    }
    fn forget(&mut self, _req: &Request<'_>, _ino: u64, _nlookup: u64) {}
    fn lseek(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        _whence: i32,
        reply: fuser::ReplyLseek,
    ) {
        reply.error(libc::ENOSYS);
    }
    fn rename(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _newparent: u64,
        _newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        reply.error(libc::ENOSYS);
    }
    fn create(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        reply.error(libc::ENOSYS);
    }
    fn symlink(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _link: &std::path::Path,
        reply: ReplyEntry,
    ) {
        reply.error(libc::ENOSYS);
    }
    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        _reply: ReplyEmpty,
    ) {
    }
    fn readlink(&mut self, _req: &Request<'_>, _ino: u64, _reply: fuser::ReplyData) {}
    fn fsyncdir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _datasync: bool,
        _reply: ReplyEmpty,
    ) {
    }
    fn setxattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        value: &[u8],
        _flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        if let Some(mut inode) = self.read_inode(ino) {
            inode
                .inner
                .xattrs
                .insert(name.to_str().unwrap().to_string(), value.to_vec());
            reply.ok();
        } else {
            reply.error(libc::EBADF);
        }
    }
    fn getxattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        size: u32,
        reply: fuser::ReplyXattr,
    ) {
        if let Some(inode) = self.read_inode(ino) {
            if let Some(data) = inode.inner.xattrs.get(name.to_str().unwrap()) {
                if size == 0 {
                    reply.size(data.len() as u32);
                } else if data.len() <= size as usize {
                    reply.data(data);
                } else {
                    reply.error(libc::ERANGE);
                }
            } else {
                reply.error(libc::ENODATA);
            }
        } else {
            reply.error(libc::EBADF);
        }
    }
    fn listxattr(&mut self, _req: &Request<'_>, ino: u64, size: u32, reply: fuser::ReplyXattr) {
        if let Some(inode) = self.read_inode(ino) {
            let mut bytes = vec![];
            for key in inode.inner.xattrs.keys() {
                bytes.extend(OsStr::new(key).as_bytes());
                bytes.push(0);
            }
            if size == 0 {
                reply.size(bytes.len() as u32);
            } else if bytes.len() <= size as usize {
                reply.data(&bytes);
            } else {
                reply.error(libc::ERANGE);
            }
        } else {
            reply.error(libc::EBADF);
        }
    }
    fn fallocate(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        _length: i64,
        _mode: i32,
        reply: ReplyEmpty,
    ) {
        reply.error(libc::ENOSYS);
    }
    fn releasedir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) {
        reply.error(libc::ENOSYS);
    }
    fn readdirplus(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        reply: fuser::ReplyDirectoryPlus,
    ) {
        reply.error(libc::ENOSYS);
    }
    fn removexattr(&mut self, _req: &Request<'_>, ino: u64, name: &OsStr, reply: ReplyEmpty) {
        if let Some(mut inode) = self.read_inode(ino) {
            if inode.inner.xattrs.remove(name.to_str().unwrap()).is_some() {
                reply.ok();
            } else {
                reply.error(libc::ENODATA);
            }
        } else {
            reply.error(libc::EBADF);
        }
    }
    fn copy_file_range(
        &mut self,
        _req: &Request<'_>,
        _ino_in: u64,
        _fh_in: u64,
        _offset_in: i64,
        _ino_out: u64,
        _fh_out: u64,
        _offset_out: i64,
        _len: u64,
        _flags: u32,
        reply: fuser::ReplyWrite,
    ) {
        reply.error(libc::ENOSYS);
    }
}
