use bitmap_allocator::{BitAlloc, BitAlloc256M};

use fuser::{
    Filesystem, KernelConfig, ReplyAttr, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyStatfs,
    Request, FUSE_ROOT_ID,
};

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::ops::Range;
use std::os::raw::c_int;
use std::os::unix::prelude::OsStrExt;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};
use std::vec;

use std::alloc::{alloc_zeroed, Layout};
pub mod block_cache;
pub mod block_dev;
pub mod inode;
use crate::inode::*;

use autocxx::prelude::*;

include_cpp! {
    #include "kv.h"
    safety!(unsafe)
    generate!("KVStore")
}

pub struct CyanFS<const BLOCK_SIZE: usize> {
    dev: Arc<Mutex<block_cache::BlockCache<BLOCK_SIZE>>>,
    meta: Arc<Mutex<InodeCache<BLOCK_SIZE>>>,
    block_allocator: Box<BitAlloc256M>,
    inode_allocator: Box<BitAlloc256M>,
}

fn new_allocator(avail: Range<usize>) -> Box<BitAlloc256M> {
    let mut allocator = unsafe {
        let layout = Layout::new::<BitAlloc256M>();
        let ptr = alloc_zeroed(layout) as *mut BitAlloc256M;
        Box::from_raw(ptr)
    };
    allocator.insert(avail);
    allocator
}

impl<const BLOCK_SIZE: usize> CyanFS<BLOCK_SIZE> {
    pub fn new(data: &str, meta: &str, new: bool, block_cache: usize, inode_cache: usize) -> Self {
        cxx::let_cxx_string!(meta = meta);
        let store = ffi::KVStore::new(&meta, new).within_unique_ptr();
        let dev = Arc::new(Mutex::new(
            block_cache::BlockCache::new(data, block_cache).unwrap(),
        ));
        Self {
            dev: dev.clone(),
            meta: Arc::new(Mutex::new(InodeCache::new(
                Arc::new(Mutex::new(store)),
                dev,
                inode_cache,
            ))),
            block_allocator: new_allocator(0..BitAlloc256M::CAP),
            inode_allocator: new_allocator(FUSE_ROOT_ID as usize..BitAlloc256M::CAP),
        }
    }
    pub fn new_with_parent<V>(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        f: impl FnOnce(&mut Attrs<BLOCK_SIZE>) -> V,
    ) -> Result<V, c_int> {
        let mut n = self.new_inode(req, None);
        let v = f(&mut n);
        let entry = DirEntry {
            ino: n.ino,
            kind: n.kind,
        };
        self.meta.lock().unwrap().insert(n);
        self.insert_dirent(parent, name, entry).map(|_| v)
    }
    pub fn new_inode(&mut self, req: &Request<'_>, ino: Option<u64>) -> Attrs<BLOCK_SIZE> {
        let now = SystemTime::now();
        Attrs {
            ino: match ino {
                Some(ino) => ino,
                None => self.inode_allocator.alloc().unwrap() as u64,
            },
            size: 0,
            extents: vec![],
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: FileType::RegularFile,
            perm: 0o777,
            nlink: 1,
            uid: req.uid(),
            gid: req.gid(),
            rdev: 0,
            flags: 0,
            link: std::path::PathBuf::new(),
            entries: BTreeMap::new(),
        }
    }
    pub fn remove_dirent(&mut self, parent: u64, name: &OsStr) -> Result<DirEntry, c_int> {
        let res = self.meta.lock().unwrap().modify(parent, |p| {
            if let Some(entry) = p.entries.remove(name.to_str().unwrap()) {
                Ok(entry)
            } else {
                Err(libc::ENOENT)
            }
        });
        res.clone().and(res.unwrap())
    }
    pub fn lookup_dirent(&mut self, parent: u64, name: &OsStr) -> Result<DirEntry, c_int> {
        let res = self.meta.lock().unwrap().read(parent, |p| {
            if let Some(entry) = p.entries.get(name.to_str().unwrap()) {
                Ok(entry.to_owned())
            } else {
                Err(libc::ENOENT)
            }
        });
        res.clone().and(res.unwrap())
    }
    pub fn insert_dirent(
        &mut self,
        parent: u64,
        name: &OsStr,
        entry: DirEntry,
    ) -> Result<(), c_int> {
        let res = self.meta.lock().unwrap().modify(parent, |p| {
            match p.entries.get(name.to_str().unwrap()) {
                None => {
                    p.entries.insert(name.to_str().unwrap().to_string(), entry);
                    Ok(())
                }
                Some(_) => Err(libc::EEXIST),
            }
        });
        res.and(res.unwrap())
    }
}

impl<const BLOCK_SIZE: usize> Filesystem for CyanFS<BLOCK_SIZE> {
    fn init(&mut self, req: &Request, _config: &mut KernelConfig) -> Result<(), c_int> {
        if self
            .meta
            .lock()
            .unwrap()
            .read(FUSE_ROOT_ID, |_| {})
            .is_err()
        {
            let mut root = self.new_inode(req, Some(FUSE_ROOT_ID));
            root.kind = FileType::Directory;
            self.meta.lock().unwrap().insert(root);
        }
        self.meta.lock().unwrap().flush();
        self.meta
            .lock()
            .unwrap()
            .scan(|i| {
                let ino = i.ino as usize;
                self.inode_allocator.remove(ino as usize..ino + 1);
                i.extents.clone().into_iter().for_each(|e| {
                    self.block_allocator.remove(e);
                })
            })
            .unwrap();
        Ok(())
    }
    fn destroy(&mut self) {
        self.meta.lock().unwrap().flush();
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
        match self.meta.lock().unwrap().read(ino, |i| {
            let mut buf = vec![0u8; size as usize];
            let size = i
                .read_at(self.dev.clone(), &mut buf, offset as u64)
                .unwrap();
            buf.truncate(size);
            buf
        }) {
            Ok(buf) => reply.data(&buf),
            Err(err) => reply.error(err),
        };
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
        match self.meta.lock().unwrap().modify(ino, |i| {
            let new_size = offset as usize + data.len();
            if new_size > i.size as usize {
                i.size = new_size as u64;
            }
            let block_cnt = (new_size + (BLOCK_SIZE - 1)) / BLOCK_SIZE;
            let origi_cnt = i.blocks();
            if block_cnt > origi_cnt {
                let cnt = block_cnt - origi_cnt;
                let begin = self
                    .block_allocator
                    .alloc_contiguous(block_cnt - origi_cnt, 0)
                    .unwrap();
                i.extents.push(begin..begin + cnt);
            }
            i.write_at(self.dev.clone(), data, offset as u64).unwrap()
        }) {
            Ok(size) => reply.written(size as u32),
            Err(err) => reply.error(err),
        };
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        match self.meta.lock().unwrap().read(ino, |i| i.into()) {
            Ok(attrs) => reply.attr(&Duration::new(0, 0), &attrs),
            Err(err) => reply.error(err),
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
        // TODO: handle error
        self.meta
            .lock()
            .unwrap()
            .read(ino, |i| {
                for (index, (name, entry)) in i.entries.iter().skip(offset as usize).enumerate() {
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
            })
            .unwrap();
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
        match self.meta.lock().unwrap().read(ino, |_| {}) {
            Ok(_) => reply.ok(),
            Err(err) => reply.error(err),
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
        match self.meta.lock().unwrap().modify(ino, |i| {
            if let Some(size) = size {
                i.size = size;
            }
            if let Some(mode) = mode {
                i.perm = mode as u16;
            }
            i.into()
        }) {
            Ok(attrs) => reply.attr(&Duration::new(0, 0), &attrs),
            Err(err) => reply.error(err),
        }
    }
    fn mknod(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        let kind = match mode & libc::S_IFMT {
            libc::S_IFREG => FileType::RegularFile,
            libc::S_IFCHR | libc::S_IFBLK | libc::S_IFIFO | libc::S_IFSOCK => {
                reply.error(libc::ENOSYS);
                return;
            }
            _ => {
                reply.error(libc::EINVAL);
                return;
            }
        };
        match self.new_with_parent(req, parent, name, |n| {
            n.perm = (mode & !umask) as u16;
            n.kind = kind;
            n.into()
        }) {
            Ok(attrs) => reply.entry(&Duration::new(0, 0), &attrs, 0),
            Err(err) => reply.error(err),
        }
    }
    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match self.remove_dirent(parent, name) {
            Ok(ent) => {
                match self.meta.lock().unwrap().modify(ent.ino, |i| {
                    i.nlink -= 1;
                    if i.nlink == 0 {
                        i.extents.clone().into_iter().for_each(|e| {
                            self.block_allocator.insert(e);
                        });
                        self.inode_allocator.dealloc(i.ino as usize);
                    }
                }) {
                    Ok(_) => reply.ok(),
                    Err(err) => reply.error(err),
                }
            }
            Err(err) => reply.error(err),
        };
    }
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let ent = self.lookup_dirent(parent, name);
        match ent {
            Ok(ent) => match self.meta.lock().unwrap().read(ent.ino, |e| e.into()) {
                Ok(attrs) => reply.entry(&Duration::new(0, 0), &attrs, 0),
                Err(err) => reply.error(err),
            },
            Err(err) => reply.error(err),
        }
    }
    fn mkdir(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        match self.new_with_parent(req, parent, name, |n| {
            n.kind = FileType::Directory;
            n.into()
        }) {
            Ok(attrs) => reply.entry(&Duration::new(0, 0), &attrs, 0),
            Err(err) => reply.error(err),
        }
    }
    fn link(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let attrs = self.meta.lock().unwrap().modify(ino, |i| {
            i.nlink += 1;
            i.to_owned()
        });
        match attrs {
            Ok(attrs) => {
                match self.insert_dirent(
                    newparent,
                    newname,
                    DirEntry {
                        ino: attrs.ino,
                        kind: attrs.kind,
                    },
                ) {
                    Ok(_) => reply.entry(&Duration::new(0, 0), &attrs.into(), 0),
                    Err(err) => reply.error(err),
                };
            }
            Err(err) => reply.error(err),
        }
    }
    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match self.remove_dirent(parent, name) {
            Ok(_) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }
    fn flush(&mut self, req: &Request<'_>, ino: u64, fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        self.fsync(req, ino, fh, true, reply)
    }
    fn fsync(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
        self.meta.lock().unwrap().flush_inode(ino);
        match self.meta.lock().unwrap().read(ino, |i| {
            i.fsync(self.dev.clone());
        }) {
            Ok(_) => reply.ok(),
            Err(err) => reply.error(err),
        };
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
        // TODO: check error
        if parent == newparent {
            self.meta
                .lock()
                .unwrap()
                .modify(parent, |p| {
                    let ent = p.entries.remove(name.to_str().unwrap()).unwrap();
                    p.entries.insert(newname.to_str().unwrap().to_string(), ent);
                })
                .unwrap();
            reply.ok();
        } else {
            let entry = self.remove_dirent(parent, name);
            if let Err(err) = entry {
                reply.error(err);
                return;
            }
            if let Err(err) = self.insert_dirent(newparent, newname, entry.unwrap()) {
                reply.error(err);
            } else {
                reply.ok();
            }
        }
    }
    fn symlink(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        link: &std::path::Path,
        reply: ReplyEntry,
    ) {
        match self.new_with_parent(req, parent, name, |n| {
            n.kind = FileType::Symlink;
            n.link = link.to_path_buf();
            n.into()
        }) {
            Ok(attrs) => reply.entry(&Duration::new(0, 0), &attrs, 0),
            Err(err) => reply.error(err),
        }
    }
    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: fuser::ReplyData) {
        match self
            .meta
            .lock()
            .unwrap()
            .read(ino, |i| i.link.as_os_str().as_bytes().to_vec())
        {
            Ok(link) => reply.data(&link),
            Err(err) => reply.error(err),
        }
    }
    fn fallocate(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        length: i64,
        _mode: i32,
        reply: ReplyEmpty,
    ) {
        match self.meta.lock().unwrap().modify(ino, |i| {
            let new_size = offset as usize + length as usize;
            if new_size > i.size as usize {
                i.size = new_size as u64;
            }
            let block_cnt = (new_size + (BLOCK_SIZE - 1)) / BLOCK_SIZE;
            let origi_cnt = i.blocks();
            if block_cnt > origi_cnt {
                let cnt = block_cnt - origi_cnt;
                let begin = self
                    .block_allocator
                    .alloc_contiguous(block_cnt - origi_cnt, 0)
                    .unwrap();
                i.extents.push(begin..begin + cnt);
            }
        }) {
            Ok(_) => reply.ok(),
            Err(err) => reply.error(err),
        };
    }
}
