//! The `rust_sketch` crate provides a Rust interface to some of the functionality of the Apache
//! `DataSketches` library. We use the datasketches-cpp implementation and provide wrappers for it.
//!
//! Currently we only have the "Quantiles Sketch" wrapper implemented, but others could be added in
//! a similar fashion.
//!
//! ## Building
//! As part of the build process, this crate needs the Apache `DataSketches` C++ code which it will
//! attempt to Git clone from [https://github.com/apache/datasketches-cpp](https://github.com/apache/datasketches-cpp) by default. If you
//! wish to override this location, please set the environment variable `RUST_SKETCH_DATASKETCH_URL`
//! to point to a new URL on build:
//! ```ignore
//! RUST_SKETCH_DATASKETCH_URL=https://some/url cargo build
//! ```
//!
//! You can also use any other valid Git protocol in here, e.g. SSH or a local path with file://
//!
//! **Note** If this crate has already cloned the datasketches-cpp repo then changing the URL to point
//! at a different repository will not trigger a new clone operation, you will need to `cargo clean` the
//! build directory first.
//!
//! ## Performance
//! Based on the assumption that sketches are updated often and read infrequently, the API design
//! has been created to allow for quick updates, minimizing copies and trying to do moves instead,
//! particularly for string and byte array sketches.
//!
//! ## Serialized compatibility
//! The serialized format of sketches in this crate should be binary compatible with the equivalent
//! Java sketches ([`ArrayOfNumbersSerDe`](https://github.com/apache/datasketches-java/blob/master/src/main/java/org/apache/datasketches/common/ArrayOfNumbersSerDe.java)
//! for i32 and i64 and [`ArrayOfStringsSerDe`](https://github.com/apache/datasketches-java/blob/master/src/main/java/org/apache/datasketches/common/ArrayOfStringsSerDe.java)
//! for String sketches). We have also implemented a byte array serializer in C++ which is used by [`quantiles::byte::byte_sketch_t`]. This
//! is directly modelled after the String serializer.
//!
//! ## Errors
//! Rust doesn't support exception handling, so functions that may trigger panics and errors from C++ are
//! wrapped to return a [`Result`].
//!
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
pub mod quantiles;
