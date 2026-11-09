fn main() {
    // Cargo's default macOS install name embeds its build directory. A stable
    // loader identity is necessary for relocation and cross-plugin rendezvous.
    match std::env::var("CARGO_CFG_TARGET_OS").as_deref() {
        Ok("macos") => {
            println!("cargo:rustc-cdylib-link-arg=-Wl,-install_name,@rpath/libcorby.dylib")
        }
        Ok("linux") => println!("cargo:rustc-cdylib-link-arg=-Wl,-soname,libcorby.so"),
        _ => {}
    }
}
