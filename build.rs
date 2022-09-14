extern crate gcc;

fn main() {
    // Build a pseudo-library so that we have symbols that we can link
    // against while building Rust code.
    // gcc::Build::new()
    //     .file("c/dict.c")
    //     .file("c/config.c")
    //     .file("c/mt19937-64.c")
    //     .file("c/redisassert.c")
    //     .file("c/zmalloc.c")
    //     .include("c/")
    //     .compile("libdict.a");
}