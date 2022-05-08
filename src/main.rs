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
        SFS::new("/tmp/sfs-meta", "/tmp/sfs-data"),
        "/tmp/sfs",
        &options,
    )
    .unwrap();
}
