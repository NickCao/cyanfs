use fuser::{
    Filesystem, KernelConfig, ReplyAttr, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyStatfs,
    Request, FUSE_ROOT_ID,
};
use rocksdb::DB;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::os::raw::c_int;
use std::os::unix::prelude::{FileExt, OsStrExt};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use std::vec;
pub mod block_cache;
pub mod block_dev;
pub mod inode;
use crate::inode::*;

const BLOCK_SIZE: usize = 512;
const CACHE_SIZE: usize = 512;

pub struct SFS {
    db: Arc<DB>,
    dev: Arc<Mutex<block_cache::BlockCache<BLOCK_SIZE, CACHE_SIZE>>>,
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
    pub fn read_inode(&self, ino: u64) -> Result<Inode, c_int> {
        Inode::lookup(self.db.clone(), self.dev.clone(), ino)
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
        let now = SystemTime::now();
        Inode {
            attrs: Attrs {
                ino: self.alloc_inode() as u64,
                size: 0,
                blocks: vec![],
                atime: now,
                mtime: now,
                ctime: now,
                crtime: now,
                kind: FileType::RegularFile,
                perm: 0o777,
                nlink: 1,
                uid: 0,
                gid: 0,
                rdev: 0,
                flags: 0,
                link: std::path::PathBuf::new(),
                entries: HashMap::new(),
            },
            db: self.db.clone(),
            dev: self.dev.clone(),
        }
    }
    pub fn remove_dirent(&mut self, parent: u64, name: &OsStr) -> Option<()> {
        if let Ok(mut inode) = self.read_inode(parent) {
            if let Some(_entry) = inode.attrs.entries.remove(name.to_str().unwrap()) {
                Some(())
            } else {
                None
            }
        } else {
            None
        }
    }
    pub fn lookup_dirent(&mut self, parent: u64, name: &OsStr) -> Option<Inode> {
        if let Ok(inode) = self.read_inode(parent) {
            inode
                .attrs
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
        inode.attrs.size = data.len() as u64;
        inode.attrs.blocks = new_blocks;
    }
    pub fn read_data(&mut self, inode: &Inode) -> Vec<u8> {
        let mut data = vec![];
        for block in &inode.attrs.blocks {
            let mut buf = [0u8; BLOCK_SIZE];
            self.dev
                .lock()
                .unwrap()
                .read_block(*block, &mut buf)
                .unwrap();
            data.extend_from_slice(&buf);
        }
        data.truncate(inode.attrs.size as usize);
        data
    }
}

impl Filesystem for SFS {
    fn init(&mut self, _req: &Request, _config: &mut KernelConfig) -> Result<(), c_int> {
        simple_logger::SimpleLogger::new().init().unwrap();
        if self.read_inode(FUSE_ROOT_ID).is_err() {
            let mut root = self.new_inode();
            root.attrs.kind = FileType::Directory;
        }
        let it = self.db.iterator(rocksdb::IteratorMode::Start);
        for (_k, v) in it {
            let inode: Attrs = bincode::deserialize(&v).unwrap();
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
    fn forget(&mut self, _req: &Request<'_>, _ino: u64, _nlookup: u64) {}
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
        assert!(offset >= 0);
        if let Ok(inode) = self.read_inode(ino) {
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
        assert!(offset >= 0);
        if let Ok(mut inode) = self.read_inode(ino) {
            let new_size = offset as usize + data.len();
            if new_size > inode.attrs.size as usize {
                inode.attrs.size = new_size as u64;
            }
            let block_cnt = (new_size + (BLOCK_SIZE - 1)) / BLOCK_SIZE;
            while block_cnt > inode.attrs.blocks.len() {
                inode.attrs.blocks.push(self.alloc_block());
            }
            let size = inode.write_at(data, offset as u64).unwrap();
            reply.written(size as u32);
        } else {
            reply.error(libc::ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        if let Ok(inode) = self.read_inode(ino) {
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
        assert!(offset >= 0);
        if let Ok(inode) = self.read_inode(ino) {
            for (index, (name, entry)) in
                inode.attrs.entries.iter().skip(offset as usize).enumerate()
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
        if self.read_inode(ino).is_ok() {
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
        if let Ok(mut inode) = self.read_inode(ino) {
            if let Some(size) = size {
                inode.attrs.size = size;
            }
            if let Some(mode) = mode {
                inode.attrs.perm = mode as u16;
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
        mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        let file_type = mode & libc::S_IFMT as u32;
        if let Ok(mut parent) = self.read_inode(parent) {
            if parent.attrs.kind != FileType::Directory {
                reply.error(libc::EACCES);
                return;
            }
            let new_inode = self.new_inode();
            if parent.attrs.entries.contains_key(name.to_str().unwrap()) {
                reply.error(libc::EEXIST);
                return;
            }
            parent.attrs.entries.insert(
                name.to_str().unwrap().to_string(),
                DirEntry {
                    ino: new_inode.attrs.ino,
                    kind: match file_type {
                        libc::S_IFREG => FileType::RegularFile,
                        libc::S_IFLNK => FileType::Symlink,
                        libc::S_IFDIR => FileType::Directory,
                        _ => {
                            reply.error(libc::ENOSYS);
                            return;
                        }
                    },
                },
            );
            reply.entry(&Duration::new(0, 0), &new_inode.into(), 0);
        } else {
            reply.error(libc::EACCES);
        };
    }
    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        if let Ok(mut inode) = self.read_inode(parent) {
            if inode.attrs.entries.remove(name.to_str().unwrap()).is_some() {
                reply.ok();
            } else {
                reply.error(libc::ENOENT);
            }
        } else {
            reply.error(libc::EBADF);
        }
    }
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
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
        if let Ok(mut parent) = self.read_inode(parent) {
            if parent.attrs.kind != FileType::Directory {
                reply.error(libc::EACCES);
                return;
            }
            let mut new_inode = self.new_inode();
            new_inode.attrs.kind = FileType::Directory;
            if parent.attrs.entries.contains_key(name.to_str().unwrap()) {
                reply.error(libc::EEXIST);
                return;
            }
            parent.attrs.entries.insert(
                name.to_str().unwrap().to_string(),
                DirEntry {
                    ino: new_inode.attrs.ino,
                    kind: FileType::Directory,
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
        if let Ok(mut inode) = self.read_inode(ino) {
            if let Ok(mut parent) = self.read_inode(newparent) {
                parent.attrs.entries.insert(
                    newname.to_str().unwrap().to_string(),
                    DirEntry {
                        ino: inode.attrs.ino,
                        kind: inode.attrs.kind,
                    },
                );
                inode.attrs.nlink += 1;
                reply.entry(&Duration::new(0, 0), &inode.into(), 0);
            }
        } else {
            reply.error(libc::EBADF);
        }
    }
    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        if let Ok(mut inode) = self.read_inode(parent) {
            if inode.attrs.entries.remove(name.to_str().unwrap()).is_some() {
                reply.ok();
            } else {
                reply.error(libc::ENOENT);
            }
        } else {
            reply.error(libc::EBADF);
        }
    }
    fn fsync(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        if let Ok(inode) = self.read_inode(ino) {
            inode
                .attrs
                .blocks
                .iter()
                .for_each(|&block| self.dev.lock().unwrap().flush_block(block));
            reply.ok();
        } else {
            reply.error(libc::EBADF);
        }
    }
    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        let inode = self.lookup_dirent(parent, name).unwrap();
        if let Ok(mut newparent) = self.read_inode(newparent) {
            newparent.attrs.entries.insert(
                newname.to_str().unwrap().to_string(),
                DirEntry {
                    ino: inode.attrs.ino,
                    kind: inode.attrs.kind,
                },
            );
            self.remove_dirent(parent, name).unwrap();
            reply.ok();
        } else {
            reply.error(libc::EACCES);
        }
    }
    fn symlink(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        link: &std::path::Path,
        reply: ReplyEntry,
    ) {
        if let Ok(mut parent) = self.read_inode(parent) {
            if parent.attrs.kind != FileType::Directory {
                reply.error(libc::EACCES);
                return;
            }
            let mut new_inode = self.new_inode();
            new_inode.attrs.kind = FileType::Symlink;
            new_inode.attrs.link = link.to_path_buf();
            if parent.attrs.entries.contains_key(name.to_str().unwrap()) {
                reply.error(libc::EEXIST);
                return;
            }
            parent.attrs.entries.insert(
                name.to_str().unwrap().to_string(),
                DirEntry {
                    ino: new_inode.attrs.ino,
                    kind: FileType::Symlink,
                },
            );
            reply.entry(&Duration::new(0, 0), &new_inode.into(), 0);
        } else {
            reply.error(libc::EBADF);
        };
    }
    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: fuser::ReplyData) {
        if let Ok(inode) = self.read_inode(ino) {
            reply.data(inode.attrs.link.as_os_str().as_bytes());
        } else {
            reply.error(libc::ENOENT);
        }
    }
}
