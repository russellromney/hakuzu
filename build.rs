fn main() {
    // No special link flags needed.
    // For turbograph extension loading, see two options:
    //
    // 1. Dynamic loading (Linux only, macOS has symbol export issues):
    //    TURBOGRAPH_EXTENSION_PATH=/path/to/libturbograph.lbug_extension
    //    cargo test --test extension_loading -- --ignored
    //
    // 2. Static linking (production, all platforms):
    //    Build lbug from source with STATICALLY_LINKED_EXTENSIONS=turbograph.
    //    The extension auto-loads on database init. No LOAD EXTENSION needed.
    //    See ladybug/extension/turbograph/CMakeLists.txt for build instructions.
}
