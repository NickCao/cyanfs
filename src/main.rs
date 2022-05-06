use fuser::mount2;
use sfs::SFS;

fn main() {
    mount2(SFS {}, "/tmp/sfs", &[]).unwrap();
}
