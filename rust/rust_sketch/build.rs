/*
 * Copyright 2022-2026 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use cargo_metadata::MetadataCommand;
use serde_json::Value;

fn main() {
    println!("cargo:rerun-if-changed=src/include");
    println!("cargo:rerun-if-changed=src/quantiles.rs");

    let datasketches_version = get_datasketch_version();
    let out_dir = std::env::var_os("CARGO_MANIFEST_DIR")
        .expect("CARGO_MANIFEST_DIR environment variable not set!");
    let path = std::path::Path::new(&out_dir)
        .join("../../vendored/datasketches-cpp")
        .join(datasketches_version);

    let version_marker_file = path.join("version.cfg.in");
    println!(
        "cargo:rerun-if-changed={}",
        version_marker_file
            .into_os_string()
            .into_string()
            .expect("Invalid UTF-8 string")
    );

    cxx_build::bridges(vec!["src/quantiles.rs"])
        .warnings_into_errors(true)
        .extra_warnings(true)
        .flag_if_supported("-std=c++17")
        .flag_if_supported("-Wno-maybe-uninitialized")
        .includes(vec![
            path.join("common/include"),
            path.join("quantiles/include"),
        ])
        .compile("rust_sketch");
}

/// Retrieve version for Apache `DataSketches` library.
///
/// Attempt to determine the version for the Apache `DataSketches` library. This is read from Cargo workspace metadata
/// `workspace.metadata.dataketches` for a `datasketches_cpp_version` key.
///
/// # Panics
/// If a version cannot be found.
fn get_datasketch_version() -> String {
    let mut command = MetadataCommand::new();
    let Ok(metadata) = command.no_deps().exec() else {
        panic!("Couldn't execute cargo metadata command.");
    };

    let Value::Object(workspace_data) = metadata.workspace_metadata else {
        panic!("Couldn't find workspace metadata.");
    };

    let Some(Value::Object(sketch_data)) = workspace_data.get("datasketches") else {
        panic!("Couldn't find \"datasketches\" metadata section.");
    };

    if let Some(Value::String(version)) = sketch_data.get("datasketches_cpp_version") {
        println!("cargo:warning=DataSketches version taken from cargo metadata: {version}");
        version.clone()
    } else {
        panic!("Couldn't find \"datasketches_cpp_version\" metadata key.");
    }
}
