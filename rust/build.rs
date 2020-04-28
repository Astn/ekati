extern crate protoc_rust;

use protoc_rust::Customize;

fn main() -> std::io::Result<()> {
    protoc_rust::run(protoc_rust::Args {
        out_dir: "src/mytypes",
        input: &["protos/types.proto"],
        includes: &["protos"],
        customize: Customize{
            carllerche_bytes_for_bytes: Some(true),
            carllerche_bytes_for_string: Some(true),
            expose_oneof:Some(true),
            expose_fields:Some(true),
            generate_accessors:Some(false)
        },
    }).expect("protoc");

    Ok(())
}