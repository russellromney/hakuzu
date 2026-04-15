fn main() {
    // On macOS, export all global symbols to the dynamic symbol table.
    // Needed for dynamically-loaded extensions (LOAD EXTENSION) to resolve
    // lbug C++ symbols at runtime via dlsym.
    //
    // Not needed for statically-linked extensions (auto-loaded at db init),
    // but doesn't hurt and supports both modes.
    if cfg!(target_os = "macos") {
        println!("cargo:rustc-link-arg=-Wl,-export_dynamic");
    }
}
