use fuser::{mount2, MountOption};
use sfs::SFS;

fn main() {
    let options = vec![
        MountOption::FSName("sfs".to_string()),
        MountOption::AutoUnmount,
    ];
    mount2(
        SFS::new(
            "/tmp/sfs-meta".to_string(),
            "/tmp/sfs-data".to_string(),
            false,
            true,
        ),
        "/tmp/sfs",
        &options,
    )
    .unwrap();
}
