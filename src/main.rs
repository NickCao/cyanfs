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
    let fs: SFS<4096> = SFS::new("target/meta", "/dev/nvme0n1p3", 2048, 2048);
    mount2(fs, "/tmp/sfs", &options).unwrap();
}
