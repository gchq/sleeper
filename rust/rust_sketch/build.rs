/*
 * Copyright 2022-2025 Crown Copyright
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
    println!("cargo:rerun-if-env-changed=RUST_SKETCH_DATASKETCH_URL");
    println!("cargo:rerun-if-env-changed=RUST_SKETCH_DATASKETCH_TAG");

    let out_dir = std::env::var_os("OUT_DIR").expect("OUT_DIR environment variable not set!");
    let path = std::path::Path::new(&out_dir);

    // look to see if the datasketches library repo has been overridden
    let url = std::env::var("RUST_SKETCH_DATASKETCH_URL")
        .unwrap_or(String::from("https://github.com/apache/datasketches-cpp"));

    // look up tag to use
    let tag = get_repo_tag();

    // try to open repository in case it already exists
    if git2::Repository::open(path.join("datasketches-cpp")).is_err() {
        // otherwise clone its repository
        println!("cargo:warning=Git cloned datasketches-cpp from {url} tag {tag}");
        let repo = match git2::Repository::clone(&url, path.join("datasketches-cpp")) {
            Ok(repo) => repo,
            Err(e) => panic!("failed to clone from {url} tag {tag}: {e}"),
        };
        {
            let reference = repo.find_reference(&format!("refs/tags/{tag}")).unwrap();
            let ob = reference.peel_to_tag().unwrap().into_object();
            repo.checkout_tree(&ob, None).unwrap();
            repo.set_head_detached(ob.id()).unwrap();
        }
    }

    cxx_build::bridges(vec!["src/quantiles.rs"])
        .warnings_into_errors(true)
        .extra_warnings(true)
        .flag_if_supported("-std=c++17")
        .includes(vec![
            path.join("datasketches-cpp/common/include"),
            path.join("datasketches-cpp/quantiles/include"),
        ])
        .compile("rust_sketch");
}

/// Retrieve git repository tag for Apache `DataSketches` library to retrieve.
///
/// Attempt to determine the git repository tag to retrieve for the Apache `DataSketches`
/// library. Checks in order:
/// 1. `RUST_SKETCH_DATASKETCH_TAG` environment variable.
/// 2. Cargo workspace metadata `workspace.metadata.dataketches` for a `git_repository_tag` key.
///
/// # Panics
/// If a repository tag cannot be found in either place.
fn get_repo_tag() -> String {
    // 1. Check environment variable
    if let Ok(env_tag) = std::env::var("RUST_SKETCH_DATASKETCH_TAG") {
        println!(
            "cargo:warning=DataSketches repository tag taken from environment variable {env_tag}"
        );
        return env_tag;
    }

    // 2. Check cargo metadata
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

    if let Some(Value::String(repo_tag)) = sketch_data.get("git_repository_tag") {
        println!(
            "cargo:warning=DataSketches repository tag taken from cargo metadata variable {repo_tag}"
        );
        repo_tag.clone()
    } else {
        panic!("Couldn't find \"git_repository_tag\" metadata key.");
    }
}
