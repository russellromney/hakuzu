fn main() {
    // On macOS, export all global symbols to the dynamic symbol table.
    // Supports both static and dynamic extension loading modes.
    if cfg!(target_os = "macos") {
        println!("cargo:rustc-link-arg=-Wl,-export_dynamic");
    }
}
