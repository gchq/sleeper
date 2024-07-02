/*
 * Copyright 2022-2024 Crown Copyright
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

    // look to see if the repo tag has been overridden
    let tag = std::env::var("RUST_SKETCH_DATASKETCH_TAG").unwrap_or(String::from("5.0.2"));

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
