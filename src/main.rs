use fuser::{mount2, MountOption};
use sfs::SFS;

fn main() {
    let options = vec![
        MountOption::FSName("sfs".to_string()),
        MountOption::AllowOther,
        MountOption::AutoUnmount,
        MountOption::DefaultPermissions,
    ];
    mount2(
        SFS::new("target/meta", "/dev/nvme0n1p3"),
        "/tmp/sfs",
        &options,
    )
    .unwrap();
}
