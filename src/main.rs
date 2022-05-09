use cannyls::nvm::FileNvm;
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
    let (nvm, _) = FileNvm::create_if_absent("target/sfs-meta", 1024 * 1024 * 1024 * 5).unwrap();
    let fs: SFS<4096, FileNvm> = SFS::new(
        "/dev/nvme0n1p3",
        2048,
        2048,
        StorageBuilder::new()
            .journal_region_ratio(0.6)
            .create(nvm)
            .unwrap(),
    );
    mount2(fs, "/tmp/sfs", &options).unwrap();
}
