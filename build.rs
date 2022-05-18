fn main() -> miette::Result<()> {
    let libkv = cmake::build("libkv");
    let path = std::path::PathBuf::from("libkv/include");
    let mut b = autocxx_build::Builder::new("src/lib.rs", &[&path]).build()?;
    b.flag_if_supported("-std=c++17").compile("binding");
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=libkv/include/kv.h");
    println!("cargo:rustc-link-search={}/lib", libkv.display());
    println!("cargo:rustc-link-lib=static=kv");
    Ok(())
}
