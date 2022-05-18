use fuser::{mount2, MountOption};
use cyanfs::CyanFS;

use argh::FromArgs;

#[derive(FromArgs)]
/// cyanfs - a poor imitation of Ceph BlueStore
struct Args {
    /// mountpoint
    #[argh(option)]
    mountpoint: String,
    /// metadata device
    #[argh(option)]
    meta: String,
    /// data device
    #[argh(option)]
    data: String,
    /// whether to create a new filesystem
    #[argh(switch)]
    new: bool,
}

fn main() {
    simple_logger::SimpleLogger::new().init().unwrap();
    let args: Args = argh::from_env();
    let options = vec![
        MountOption::FSName("cyanfs".to_string()),
        MountOption::AllowOther,
        MountOption::AutoUnmount,
        MountOption::DefaultPermissions,
    ];
    let fs: CyanFS<512> = CyanFS::new(&args.data, &args.meta, args.new, 2048, 2048);
    mount2(fs, args.mountpoint, &options).unwrap();
}
