use cannyls::nvm::MemoryNvm;
use cannyls::storage::StorageBuilder;
use fuser::{mount2, MountOption};
use sfs::SFS;

fn main() {
    simple_logger::SimpleLogger::new().init().unwrap();
    let options = vec![
        MountOption::FSName("sfs".to_string()),
        MountOption::AllowOther,
        MountOption::AutoUnmount,
        MountOption::DefaultPermissions,
    ];
    let fs: SFS<4096, MemoryNvm> = SFS::new(
        "/dev/nvme0n1p3",
        2048,
        2048,
        StorageBuilder::new()
            .journal_region_ratio(0.6)
            .create(MemoryNvm::new(vec![0; 1024 * 1024 * 1024 * 5]))
            .unwrap(),
    );
    mount2(fs, "/tmp/sfs", &options).unwrap();
}
