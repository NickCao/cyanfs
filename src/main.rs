use fuser::{mount2, MountOption};
use sfs::SFS;

fn main() {
    let options = vec![
        MountOption::FSName("sfs".to_string()),
        MountOption::AllowOther,
        MountOption::AutoUnmount,
        MountOption::DefaultPermissions,
    ];
    let fs: SFS<512> = SFS::new("target/meta", "/dev/nvme0n1p3");
    mount2(fs, "/tmp/sfs", &options).unwrap();
}
