use cannyls::nvm::FileNvm;
use cannyls::storage::StorageBuilder;
use fuser::{mount2, MountOption};
use sfs::SFS;

use argh::FromArgs;

#[derive(FromArgs)]
/// sfs - a poor imitation of Ceph BlueStore
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
        MountOption::FSName("sfs".to_string()),
        MountOption::AllowOther,
        MountOption::AutoUnmount,
        MountOption::DefaultPermissions,
    ];
    let mut store = if args.new {
        let (nvm, _) = FileNvm::create_if_absent(args.meta, 1024 * 1024 * 1024 * 5).unwrap();
        StorageBuilder::new()
            .journal_region_ratio(0.6)
            .create(nvm)
            .unwrap()
    } else {
        let nvm = FileNvm::open(args.meta).unwrap();
        StorageBuilder::new().open(nvm).unwrap()
    };
    store.run_side_job_once().unwrap();
    let fs: SFS<4096, FileNvm> = SFS::new(&args.data, 2048, 2048, store);
    mount2(fs, args.mountpoint, &options).unwrap();
}
