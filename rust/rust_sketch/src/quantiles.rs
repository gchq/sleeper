//! Provides wrappers for quantile sketches.
//!
//! Since support for generic trait parameters is not yet available in the CXX
//! bridge, we have to manually instantiate various sketch types. The following 4 types are implemented:
//!
//! The following 4 types are implemented:
//!
//! * i32 sketch
//! * i64 sketch
//! * string type (`str`)
//! * byte array type (`Vec<u8>`)
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
//! for String sketches). We have also implemented a byte array serializer in C++ which is used by [`byte::byte_sketch_t`]. This
//! is directly modelled after the String serializer.
//!
//! ## Errors
//! Rust doesn't support exception handling, so functions that may trigger panics and errors from C++ are
//! wrapped to return a [`Result`].
//!
//! For ease of maintenance the 4 types are split across public sub-modules.
//! # Examples
//! ```
//! // Make a simple byte array sketch
//! let mut sketch = rust_sketch::quantiles::byte::new_byte_sketch(1024);
//!
//! // Populate it
//! for i in b'\x21'..b'\x7e' {
//!     sketch.pin_mut().update(&[i]);
//! }
//!
//! println!(
//!     "Sketch contains {} values, median {}",
//!     sketch.get_k(),
//!     std::str::from_utf8(&sketch.get_quantile(0.5, true).unwrap()).unwrap()
//! );
//!
//! // Serialise it and then deserialise it
//! let serial = sketch.serialize(0).expect("Serialise fail");
//! let recreate = rust_sketch::quantiles::byte::byte_deserialize(&serial).expect("Deserialise fail");
//!
//! // Check things match!
//! assert_eq!(
//!     sketch.get_quantile(0.4, true).unwrap(),
//!     recreate.get_quantile(0.4, true).unwrap()
//! );
//! ```
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
/// Provides a quantiles sketch for the `i32` primitive type.
#[cxx::bridge(namespace = "rust_sketch")]
#[allow(
    clippy::module_name_repetitions,
    clippy::must_use_candidate,
    clippy::missing_errors_doc
)]
pub mod i32 {

    unsafe extern "C++" {
        include!("rust_sketch/src/include/quantiles.hpp");
        /// Rust façade for C++ `datasketches::quantile_sketch<int32_t>`
        type i32_sketch_t; // trigger instantiation of template quantiles_sketch_derived<::int32_t>;

        /// Create a new quantiles sketch.
        ///
        /// This will create a new quantiles sketch object and wrap it in a
        /// [`cxx::UniquePtr`] (a wrapper for [`std::unique_ptr`](https://en.cppreference.com/w/cpp/memory/unique_ptr)).
        /// This ensures that ownership rules are enforced (on both Rust and C++) side and prevent memory leaks.
        /// All interaction must be done through the pointer returned through this function.
        ///
        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for full algorithm details and how the parameters affect behaviour.
        ///
        /// # Example
        /// ```
        /// let sketch = rust_sketch::quantiles::i32::new_i32_sketch(1024);
        /// ```
        ///
        /// # Panics
        /// If memory allocation for the sketch object fails in C++.
        #[rust_name = "new_i32_sketch"]
        fn new_quantiles_sketch(K: u16) -> UniquePtr<i32_sketch_t>;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        fn update(self: Pin<&mut i32_sketch_t>, value: i32);

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        /// # Errors
        /// Rare, but might throw a consistency error if the C++ layer experienced an internal consistency fault.
        fn merge(self: Pin<&mut i32_sketch_t>, other: &i32_sketch_t) -> Result<()>;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        /// # Errors
        /// If called on an empty then an error will be returned.
        fn get_min_item(self: &i32_sketch_t) -> Result<i32>;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        /// # Errors
        /// If called on an empty then an error will be returned.
        fn get_max_item(self: &i32_sketch_t) -> Result<i32>;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        fn is_empty(self: &i32_sketch_t) -> bool;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        fn get_k(self: &i32_sketch_t) -> u16;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        fn get_n(self: &i32_sketch_t) -> u64;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        fn get_num_retained(self: &i32_sketch_t) -> u32;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        fn is_estimation_mode(self: &i32_sketch_t) -> bool;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        /// # Errors
        /// If called on an empty then an error will be returned.
        fn get_quantile(self: &i32_sketch_t, rank: f64, inclusive: bool) -> Result<i32>;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        /// # Errors
        /// If called on an empty then an error will be returned.
        fn get_rank(self: &i32_sketch_t, item: i32, inclusive: bool) -> Result<f64>;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        fn get_normalized_rank_error(self: &i32_sketch_t, is_pmf: bool) -> f64;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        fn get_serialized_size_bytes(self: &i32_sketch_t) -> usize;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        /// # Errors
        /// If an error in the serialization stream occurs, then the exception is wrapped in the error message.
        fn serialize(self: &i32_sketch_t, header_size_bytes: u32) -> Result<Vec<u8>>;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        /// # Errors
        /// If an error in the deserialization stream occurs, then the exception is wrapped in the error message.
        #[rust_name = "i32_deserialize"]
        fn deserialize(bytes: &[u8]) -> Result<UniquePtr<i32_sketch_t>>;

    }
}

unsafe impl Send for i32::i32_sketch_t {}

#[cfg(test)]
mod i32_tests {

    const N: i32 = 1_000_000;
    const ALLOWED_F64_EPSILON: f64 = 0.01;

    use super::{
        i32::{i32_deserialize, i32_sketch_t},
        *,
    };
    use cxx::UniquePtr;
    use rand::RngExt;

    // convenience alias
    use i32_sketch_t as sketch_type;

    fn create_empty() -> UniquePtr<sketch_type> {
        i32::new_i32_sketch(1024)
    }

    fn create_single_entry() -> UniquePtr<sketch_type> {
        let mut sketch = create_empty();
        sketch.pin_mut().update(10);
        sketch
    }

    fn create_multiple_entries() -> UniquePtr<sketch_type> {
        let mut sketch = create_empty();
        for i in 1..10 {
            sketch.pin_mut().update(i);
        }
        sketch
    }

    fn check_approx_equal(lhs: &UniquePtr<sketch_type>, rhs: &UniquePtr<sketch_type>) {
        assert_eq!(lhs.is_empty(), rhs.is_empty());
        assert_eq!(lhs.get_k(), rhs.get_k());
        assert_eq!(lhs.get_n(), rhs.get_n());
        assert!(
            (lhs.get_normalized_rank_error(true) - rhs.get_normalized_rank_error(true)).abs()
                < ALLOWED_F64_EPSILON
        );
        assert_eq!(lhs.get_num_retained(), rhs.get_num_retained());

        if !lhs.is_empty() && !rhs.is_empty() {
            assert_eq!(lhs.get_min_item().unwrap(), rhs.get_min_item().unwrap());
            assert_eq!(lhs.get_max_item().unwrap(), rhs.get_max_item().unwrap());
            let mut f = 0.0;
            while f < 1.0 {
                assert_eq!(
                    lhs.get_quantile(f, true).unwrap(),
                    rhs.get_quantile(f, true).unwrap()
                );
                f += 0.1;
            }
        }
    }

    #[test]
    fn creates() {
        create_empty();
    }

    #[test]
    fn empty_sketch_returns_errors() {
        let sketch = create_empty();
        assert!(sketch.get_min_item().is_err());
        assert!(sketch.get_max_item().is_err());
        assert!(sketch.get_quantile(0.0, true).is_err());
        assert!(sketch.get_rank(0, true).is_err());
    }

    #[test]
    fn empty_sketch_is_empty() {
        assert!(create_empty().is_empty());
    }

    #[test]
    fn empty_sketch_contains_zero() {
        assert_eq!(create_empty().get_n(), 0);
    }

    #[test]
    fn empty_sketch_serialize() {
        let sketch = create_empty();
        let serial = sketch.serialize(0).unwrap();
        let deserial = i32_deserialize(&serial).unwrap();
        check_approx_equal(&sketch, &deserial);
    }

    #[test]
    fn single_sketch_returns_min() {
        let sketch = create_single_entry();
        assert_eq!(sketch.get_min_item().unwrap(), 10);
    }

    #[test]
    fn single_sketch_returns_max() {
        let sketch = create_single_entry();
        assert_eq!(sketch.get_max_item().unwrap(), 10);
    }

    #[test]
    fn single_sketch_returns_quantile() {
        let sketch = create_single_entry();
        assert_eq!(sketch.get_quantile(0.5, true).unwrap(), 10);
    }

    #[test]
    fn single_sketch_returns_rank() {
        let sketch = create_single_entry();
        assert!((sketch.get_rank(10, true).unwrap() - 1.0).abs() < ALLOWED_F64_EPSILON);
    }

    #[test]
    fn single_sketch_not_empty() {
        assert!(!create_single_entry().is_empty());
    }

    #[test]
    fn single_sketch_contains_one() {
        assert_eq!(create_single_entry().get_n(), 1);
    }

    #[test]
    fn single_sketch_serialize() {
        let sketch = create_single_entry();
        let serial = sketch.serialize(0).unwrap();
        let deserial = i32_deserialize(&serial).unwrap();
        check_approx_equal(&sketch, &deserial);
    }

    #[test]
    fn empty_merge_stays_empty() {
        let mut sketch_l = create_empty();
        let sketch_r = create_empty();
        sketch_l.pin_mut().merge(&sketch_r).unwrap();
        assert!(sketch_l.is_empty());
    }

    #[test]
    fn empty_merge_single_is_single() {
        let mut sketch_l = create_empty();
        let sketch_r = create_single_entry();
        sketch_l.pin_mut().merge(&sketch_r).unwrap();
        assert_eq!(sketch_l.get_n(), 1);
        check_approx_equal(&sketch_l, &sketch_r);
    }

    #[test]
    fn single_merge_empty_is_single() {
        let mut sketch_l = create_single_entry();
        let sketch_r = create_empty();
        sketch_l.pin_mut().merge(&sketch_r).unwrap();
        assert_eq!(sketch_l.get_n(), 1);
        assert_eq!(
            sketch_l.get_min_item().unwrap(),
            sketch_l.get_max_item().unwrap()
        );
        assert_eq!(sketch_l.get_num_retained(), 1);
    }

    #[test]
    fn multi_sketch_merge() {
        let mut sketch_l = create_multiple_entries();
        let sketch_r = create_multiple_entries();
        sketch_l.pin_mut().merge(&sketch_r).unwrap();
        assert_eq!(sketch_l.get_n(), sketch_r.get_n() * 2);

        let mut expected = create_empty();
        for i in 1..10 {
            expected.pin_mut().update(i);
            expected.pin_mut().update(i);
        }
        check_approx_equal(&sketch_l, &expected);
    }

    #[test]
    fn multi_sketch_returns_min() {
        let sketch = create_multiple_entries();
        assert_eq!(sketch.get_min_item().unwrap(), 1);
    }

    #[test]
    fn multi_sketch_returns_max() {
        let sketch = create_multiple_entries();
        assert_eq!(sketch.get_max_item().unwrap(), 9);
    }

    #[test]
    fn multi_sketch_returns_quantile() {
        let sketch = create_multiple_entries();
        assert_eq!(sketch.get_quantile(0.5, true).unwrap(), 5);
    }

    #[test]
    fn multi_sketch_returns_rank() {
        let sketch = create_multiple_entries();
        assert!((sketch.get_rank(0, true).unwrap() - 0.0).abs() < ALLOWED_F64_EPSILON);
    }

    #[test]
    fn multi_sketch_not_empty() {
        assert!(!create_multiple_entries().is_empty());
    }

    #[test]
    fn multi_sketch_contains_nine() {
        assert_eq!(create_multiple_entries().get_n(), 9);
    }

    #[test]
    fn multi_sketch_serialize() {
        let sketch = create_multiple_entries();
        let serial = sketch.serialize(0).unwrap();
        let deserial = i32_deserialize(&serial).unwrap();
        check_approx_equal(&sketch, &deserial);
    }

    #[test]
    fn test_serialize_random() {
        let mut sketch = create_empty();
        let mut rng = rand::rng();

        for _ in 0..N {
            let r: i32 = rng.random();
            sketch.pin_mut().update(r);
        }

        let serial = sketch.serialize(0).unwrap();
        let deserial = i32_deserialize(&serial).unwrap();
        check_approx_equal(&sketch, &deserial);
    }
}

/// Provides a quantiles sketch for the `i64` primitive type.
#[cxx::bridge(namespace = "rust_sketch")]
#[allow(
    clippy::module_name_repetitions,
    clippy::must_use_candidate,
    clippy::missing_errors_doc
)]
pub mod i64 {
    unsafe extern "C++" {
        include!("rust_sketch/src/include/quantiles.hpp");
        /// Rust façade for C++ `datasketches::quantile_sketch<int64_t>`
        type i64_sketch_t; // trigger instantiation of template quantiles_sketch_derived<::int64_t>;

        /// Create a new quantiles sketch.
        ///
        /// This will create a new quantiles sketch object and wrap it in a
        /// [`cxx::UniquePtr`] (a wrapper for [`std::unique_ptr`](https://en.cppreference.com/w/cpp/memory/unique_ptr)).
        /// This ensures that ownership rules are enforced (on both Rust and C++) side and prevent memory leaks.
        /// All interaction must be done through the pointer returned through this function.
        ///
        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for full algorithm details and how the parameters affect behaviour.
        ///
        /// # Example
        /// ```
        /// let sketch = rust_sketch::quantiles::i64::new_i64_sketch(1024);
        /// ```
        ///
        /// # Panics
        /// If memory allocation for the sketch object fails in C++.
        #[rust_name = "new_i64_sketch"]
        fn new_quantiles_sketch(K: u16) -> UniquePtr<i64_sketch_t>;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        fn update(self: Pin<&mut i64_sketch_t>, value: i64);

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        /// # Errors
        /// Rare, but might throw a consistency error if the C++ layer experienced an internal consistency fault.
        fn merge(self: Pin<&mut i64_sketch_t>, other: &i64_sketch_t) -> Result<()>;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        /// # Errors
        /// If called on an empty then an error will be returned.
        fn get_min_item(self: &i64_sketch_t) -> Result<i64>;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        /// # Errors
        /// If called on an empty then an error will be returned.
        fn get_max_item(self: &i64_sketch_t) -> Result<i64>;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        fn is_empty(self: &i64_sketch_t) -> bool;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        fn get_k(self: &i64_sketch_t) -> u16;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        fn get_n(self: &i64_sketch_t) -> u64;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        fn get_num_retained(self: &i64_sketch_t) -> u32;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        fn is_estimation_mode(self: &i64_sketch_t) -> bool;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        /// # Errors
        /// If called on an empty then an error will be returned.
        fn get_quantile(self: &i64_sketch_t, rank: f64, inclusive: bool) -> Result<i64>;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        /// # Errors
        /// If called on an empty then an error will be returned.
        fn get_rank(self: &i64_sketch_t, item: i64, inclusive: bool) -> Result<f64>;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        fn get_normalized_rank_error(self: &i64_sketch_t, is_pmf: bool) -> f64;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        fn get_serialized_size_bytes(self: &i64_sketch_t) -> usize;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        /// # Errors
        /// If an error in the serialization stream occurs, then the exception is wrapped in the error message.
        fn serialize(self: &i64_sketch_t, header_size_bytes: u32) -> Result<Vec<u8>>;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        /// # Errors
        /// If an error in the deserialization stream occurs, then the exception is wrapped in the error message.
        #[rust_name = "i64_deserialize"]
        fn deserialize(bytes: &[u8]) -> Result<UniquePtr<i64_sketch_t>>;
    }
}

unsafe impl Send for i64::i64_sketch_t {}

#[cfg(test)]
mod i64_tests {

    const N: i32 = 1_000_000;
    const ALLOWED_F64_EPSILON: f64 = 0.01;

    use super::{
        i64::{i64_deserialize, i64_sketch_t},
        *,
    };
    use cxx::UniquePtr;
    use rand::RngExt;

    // convenience alias
    use i64_sketch_t as sketch_type;

    fn create_empty() -> UniquePtr<sketch_type> {
        i64::new_i64_sketch(1024)
    }

    fn create_single_entry() -> UniquePtr<sketch_type> {
        let mut sketch = create_empty();
        sketch.pin_mut().update(10);
        sketch
    }

    fn create_multiple_entries() -> UniquePtr<sketch_type> {
        let mut sketch = create_empty();
        for i in 1..10 {
            sketch.pin_mut().update(i);
        }
        sketch
    }

    fn check_approx_equal(lhs: &UniquePtr<sketch_type>, rhs: &UniquePtr<sketch_type>) {
        assert_eq!(lhs.is_empty(), rhs.is_empty());
        assert_eq!(lhs.get_k(), rhs.get_k());
        assert_eq!(lhs.get_n(), rhs.get_n());
        assert!(
            (lhs.get_normalized_rank_error(true) - rhs.get_normalized_rank_error(true)).abs()
                < ALLOWED_F64_EPSILON
        );
        assert_eq!(lhs.get_num_retained(), rhs.get_num_retained());

        if !lhs.is_empty() && !rhs.is_empty() {
            assert_eq!(lhs.get_min_item().unwrap(), rhs.get_min_item().unwrap());
            assert_eq!(lhs.get_max_item().unwrap(), rhs.get_max_item().unwrap());
            let mut f = 0.0;
            while f < 1.0 {
                assert_eq!(
                    lhs.get_quantile(f, true).unwrap(),
                    rhs.get_quantile(f, true).unwrap()
                );
                f += 0.1;
            }
        }
    }

    #[test]
    fn creates() {
        create_empty();
    }

    #[test]
    fn empty_sketch_returns_errors() {
        let sketch = create_empty();
        assert!(sketch.get_min_item().is_err());
        assert!(sketch.get_max_item().is_err());
        assert!(sketch.get_quantile(0.0, true).is_err());
        assert!(sketch.get_rank(0, true).is_err());
    }

    #[test]
    fn empty_sketch_is_empty() {
        assert!(create_empty().is_empty());
    }

    #[test]
    fn empty_sketch_contains_zero() {
        assert_eq!(create_empty().get_n(), 0);
    }

    #[test]
    fn empty_sketch_serialize() {
        let sketch = create_empty();
        let serial = sketch.serialize(0).unwrap();
        let deserial = i64_deserialize(&serial).unwrap();
        check_approx_equal(&sketch, &deserial);
    }

    #[test]
    fn single_sketch_returns_min() {
        let sketch = create_single_entry();
        assert_eq!(sketch.get_min_item().unwrap(), 10);
    }

    #[test]
    fn single_sketch_returns_max() {
        let sketch = create_single_entry();
        assert_eq!(sketch.get_max_item().unwrap(), 10);
    }

    #[test]
    fn single_sketch_returns_quantile() {
        let sketch = create_single_entry();
        assert_eq!(sketch.get_quantile(0.5, true).unwrap(), 10);
    }

    #[test]
    fn single_sketch_returns_rank() {
        let sketch = create_single_entry();
        assert!((sketch.get_rank(10, true).unwrap() - 1.0).abs() < ALLOWED_F64_EPSILON);
    }

    #[test]
    fn single_sketch_not_empty() {
        assert!(!create_single_entry().is_empty());
    }

    #[test]
    fn single_sketch_contains_one() {
        assert_eq!(create_single_entry().get_n(), 1);
    }

    #[test]
    fn single_sketch_serialize() {
        let sketch = create_single_entry();
        let serial = sketch.serialize(0).unwrap();
        let deserial = i64_deserialize(&serial).unwrap();
        check_approx_equal(&sketch, &deserial);
    }

    #[test]
    fn empty_merge_stays_empty() {
        let mut sketch_l = create_empty();
        let sketch_r = create_empty();
        sketch_l.pin_mut().merge(&sketch_r).unwrap();
        assert!(sketch_l.is_empty());
    }

    #[test]
    fn empty_merge_single_is_single() {
        let mut sketch_l = create_empty();
        let sketch_r = create_single_entry();
        sketch_l.pin_mut().merge(&sketch_r).unwrap();
        assert_eq!(sketch_l.get_n(), 1);
        check_approx_equal(&sketch_l, &sketch_r);
    }

    #[test]
    fn single_merge_empty_is_single() {
        let mut sketch_l = create_single_entry();
        let sketch_r = create_empty();
        sketch_l.pin_mut().merge(&sketch_r).unwrap();
        assert_eq!(sketch_l.get_n(), 1);
        assert_eq!(
            sketch_l.get_min_item().unwrap(),
            sketch_l.get_max_item().unwrap()
        );
        assert_eq!(sketch_l.get_num_retained(), 1);
    }

    #[test]
    fn multi_sketch_merge() {
        let mut sketch_l = create_multiple_entries();
        let sketch_r = create_multiple_entries();
        sketch_l.pin_mut().merge(&sketch_r).unwrap();
        assert_eq!(sketch_l.get_n(), sketch_r.get_n() * 2);

        let mut expected = create_empty();
        for i in 1..10 {
            expected.pin_mut().update(i);
            expected.pin_mut().update(i);
        }
        check_approx_equal(&sketch_l, &expected);
    }
    #[test]
    fn multi_sketch_returns_min() {
        let sketch = create_multiple_entries();
        assert_eq!(sketch.get_min_item().unwrap(), 1);
    }

    #[test]
    fn multi_sketch_returns_max() {
        let sketch = create_multiple_entries();
        assert_eq!(sketch.get_max_item().unwrap(), 9);
    }

    #[test]
    fn multi_sketch_returns_quantile() {
        let sketch = create_multiple_entries();
        assert_eq!(sketch.get_quantile(0.5, true).unwrap(), 5);
    }

    #[test]
    fn multi_sketch_returns_rank() {
        let sketch = create_multiple_entries();
        assert!((sketch.get_rank(0, true).unwrap() - 0.0).abs() < ALLOWED_F64_EPSILON);
    }

    #[test]
    fn multi_sketch_not_empty() {
        assert!(!create_multiple_entries().is_empty());
    }

    #[test]
    fn multi_sketch_contains_nine() {
        assert_eq!(create_multiple_entries().get_n(), 9);
    }

    #[test]
    fn multi_sketch_serialize() {
        let sketch = create_multiple_entries();
        let serial = sketch.serialize(0).unwrap();
        let deserial = i64_deserialize(&serial).unwrap();
        check_approx_equal(&sketch, &deserial);
    }

    #[test]
    fn test_serialize_random() {
        let mut sketch = create_empty();
        let mut rng = rand::rng();

        for _ in 0..N {
            let r: i64 = rng.random();
            sketch.pin_mut().update(r);
        }

        let serial = sketch.serialize(0).unwrap();
        let deserial = i64_deserialize(&serial).unwrap();
        check_approx_equal(&sketch, &deserial);
    }
}

/// Provides a quantiles sketch for the `str` primitive type.
#[cxx::bridge(namespace = "rust_sketch")]
#[allow(
    clippy::module_name_repetitions,
    clippy::must_use_candidate,
    clippy::missing_errors_doc
)]
pub mod str {
    unsafe extern "C++" {
        include!("rust_sketch/src/include/quantiles.hpp");
        /// Rust façade for C++ `datasketches::quantile_sketch<std::string>`
        type string_sketch_t; // trigger instantiation of template quantiles_sketch_derived<std::string>;

        /// Create a new quantiles sketch.
        ///
        /// This will create a new quantiles sketch object and wrap it in a
        /// [`cxx::UniquePtr`] (a wrapper for [`std::unique_ptr`](https://en.cppreference.com/w/cpp/memory/unique_ptr)).
        /// This ensures that ownership rules are enforced (on both Rust and C++) side and prevent memory leaks.
        /// All interaction must be done through the pointer returned through this function.
        ///
        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for full algorithm details and how the parameters affect behaviour.
        ///
        /// # Example
        /// ```
        /// let sketch = rust_sketch::quantiles::str::new_str_sketch(1024);
        /// ```
        ///
        /// # Panics
        /// If memory allocation for the sketch object fails in C++.
        #[rust_name = "new_str_sketch"]
        fn new_quantiles_sketch(K: u16) -> UniquePtr<string_sketch_t>;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        fn update(self: Pin<&mut string_sketch_t>, value: &str);

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        /// # Errors
        /// Rare, but might throw a consistency error if the C++ layer experienced an internal consistency fault.
        fn merge(self: Pin<&mut string_sketch_t>, other: &string_sketch_t) -> Result<()>;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        /// # Errors
        /// If called on an empty then an error will be returned.
        fn get_min_item(self: &string_sketch_t) -> Result<String>;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        /// # Errors
        /// If called on an empty then an error will be returned.
        fn get_max_item(self: &string_sketch_t) -> Result<String>;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        fn is_empty(self: &string_sketch_t) -> bool;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        fn get_k(self: &string_sketch_t) -> u16;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        fn get_n(self: &string_sketch_t) -> u64;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        fn get_num_retained(self: &string_sketch_t) -> u32;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        fn is_estimation_mode(self: &string_sketch_t) -> bool;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        /// # Errors
        /// If called on an empty then an error will be returned.
        fn get_quantile(self: &string_sketch_t, rank: f64, inclusive: bool) -> Result<String>;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        /// # Errors
        /// If called on an empty then an error will be returned.
        fn get_rank(self: &string_sketch_t, item: &str, inclusive: bool) -> Result<f64>;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        fn get_normalized_rank_error(self: &string_sketch_t, is_pmf: bool) -> f64;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        fn get_serialized_size_bytes(self: &string_sketch_t) -> usize;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        /// # Errors
        /// If an error in the serialization stream occurs, then the exception is wrapped in the error message.
        fn serialize(self: &string_sketch_t, header_size_bytes: u32) -> Result<Vec<u8>>;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        /// # Errors
        /// If an error in the deserialization stream occurs, then the exception is wrapped in the error message.
        #[rust_name = "str_deserialize"]
        fn deserialize(bytes: &[u8]) -> Result<UniquePtr<string_sketch_t>>;
    }
}

unsafe impl Send for str::string_sketch_t {}

#[cfg(test)]
mod str_tests {

    const N: i32 = 100_000;
    const ALLOWED_F64_EPSILON: f64 = 0.01;

    use super::{
        str::{str_deserialize, string_sketch_t},
        *,
    };
    use cxx::UniquePtr;
    use rand::distr::{Alphanumeric, SampleString};

    // convenience alias
    use string_sketch_t as sketch_type;

    fn create_empty() -> UniquePtr<sketch_type> {
        str::new_str_sketch(1024)
    }

    fn create_single_entry() -> UniquePtr<sketch_type> {
        let mut sketch = create_empty();
        sketch.pin_mut().update("hello");
        sketch
    }

    fn create_multiple_entries() -> UniquePtr<sketch_type> {
        let mut sketch = create_empty();
        for i in 'a'..='z' {
            sketch
                .pin_mut()
                .update(&String::from_utf8(vec![i as u8]).unwrap());
        }
        sketch
    }

    fn check_approx_equal(lhs: &UniquePtr<sketch_type>, rhs: &UniquePtr<sketch_type>) {
        assert_eq!(lhs.is_empty(), rhs.is_empty());
        assert_eq!(lhs.get_k(), rhs.get_k());
        assert_eq!(lhs.get_n(), rhs.get_n());
        assert!(
            (lhs.get_normalized_rank_error(true) - rhs.get_normalized_rank_error(true)).abs()
                < ALLOWED_F64_EPSILON
        );
        assert_eq!(lhs.get_num_retained(), rhs.get_num_retained());

        if !lhs.is_empty() && !rhs.is_empty() {
            assert_eq!(lhs.get_min_item().unwrap(), rhs.get_min_item().unwrap());
            assert_eq!(lhs.get_max_item().unwrap(), rhs.get_max_item().unwrap());
            let mut f = 0.0;
            while f < 1.0 {
                assert_eq!(
                    lhs.get_quantile(f, true).unwrap(),
                    rhs.get_quantile(f, true).unwrap()
                );
                f += 0.1;
            }
        }
    }

    #[test]
    fn creates() {
        create_empty();
    }

    #[test]
    fn empty_sketch_returns_errors() {
        let sketch = create_empty();
        assert!(sketch.get_min_item().is_err());
        assert!(sketch.get_max_item().is_err());
        assert!(sketch.get_quantile(0.0, true).is_err());
        assert!(sketch.get_rank("", true).is_err());
    }

    #[test]
    fn empty_sketch_is_empty() {
        assert!(create_empty().is_empty());
    }

    #[test]
    fn empty_sketch_contains_zero() {
        assert_eq!(create_empty().get_n(), 0);
    }

    #[test]
    fn empty_sketch_serialize() {
        let sketch = create_empty();
        let serial = sketch.serialize(0).unwrap();
        let deserial = str_deserialize(&serial).unwrap();
        check_approx_equal(&sketch, &deserial);
    }

    #[test]
    fn single_sketch_returns_min() {
        let sketch = create_single_entry();
        assert_eq!(sketch.get_min_item().unwrap(), "hello");
    }

    #[test]
    fn single_sketch_returns_max() {
        let sketch = create_single_entry();
        assert_eq!(sketch.get_max_item().unwrap(), "hello");
    }

    #[test]
    fn single_sketch_returns_quantile() {
        let sketch = create_single_entry();
        assert_eq!(sketch.get_quantile(0.5, true).unwrap(), "hello");
    }

    #[test]
    fn single_sketch_returns_rank() {
        let sketch = create_single_entry();
        assert!((sketch.get_rank("hello", true).unwrap() - 1.0).abs() < ALLOWED_F64_EPSILON);
    }

    #[test]
    fn single_sketch_not_empty() {
        assert!(!create_single_entry().is_empty());
    }

    #[test]
    fn single_sketch_contains_one() {
        assert_eq!(create_single_entry().get_n(), 1);
    }

    #[test]
    fn single_sketch_serialize() {
        let sketch = create_single_entry();
        let serial = sketch.serialize(0).unwrap();
        let deserial = str_deserialize(&serial).unwrap();
        check_approx_equal(&sketch, &deserial);
    }

    #[test]
    fn empty_merge_stays_empty() {
        let mut sketch_l = create_empty();
        let sketch_r = create_empty();
        sketch_l.pin_mut().merge(&sketch_r).unwrap();
        assert!(sketch_l.is_empty());
    }

    #[test]
    fn empty_merge_single_is_single() {
        let mut sketch_l = create_empty();
        let sketch_r = create_single_entry();
        sketch_l.pin_mut().merge(&sketch_r).unwrap();
        assert_eq!(sketch_l.get_n(), 1);
        check_approx_equal(&sketch_l, &sketch_r);
    }

    #[test]
    fn single_merge_empty_is_single() {
        let mut sketch_l = create_single_entry();
        let sketch_r = create_empty();
        sketch_l.pin_mut().merge(&sketch_r).unwrap();
        assert_eq!(sketch_l.get_n(), 1);
        assert_eq!(
            sketch_l.get_min_item().unwrap(),
            sketch_l.get_max_item().unwrap()
        );
        assert_eq!(sketch_l.get_num_retained(), 1);
    }

    #[test]
    fn multi_sketch_merge() {
        let mut sketch_l = create_multiple_entries();
        let sketch_r = create_multiple_entries();
        sketch_l.pin_mut().merge(&sketch_r).unwrap();
        assert_eq!(sketch_l.get_n(), sketch_r.get_n() * 2);

        let mut expected = create_empty();
        for i in 'a'..='z' {
            expected
                .pin_mut()
                .update(&String::from_utf8(vec![i as u8]).unwrap());
            expected
                .pin_mut()
                .update(&String::from_utf8(vec![i as u8]).unwrap());
        }
        check_approx_equal(&sketch_l, &expected);
    }

    #[test]
    fn multi_sketch_returns_min() {
        let sketch = create_multiple_entries();
        assert_eq!(sketch.get_min_item().unwrap(), "a");
    }

    #[test]
    fn multi_sketch_returns_max() {
        let sketch = create_multiple_entries();
        assert_eq!(sketch.get_max_item().unwrap(), "z");
    }

    #[test]
    fn multi_sketch_returns_quantile() {
        let sketch = create_multiple_entries();
        assert_eq!(sketch.get_quantile(0.5, true).unwrap(), "m");
    }

    #[test]
    fn multi_sketch_returns_rank() {
        let sketch = create_multiple_entries();
        assert!((sketch.get_rank("a", true).unwrap() - 0.04).abs() < ALLOWED_F64_EPSILON);
    }

    #[test]
    fn multi_sketch_not_empty() {
        assert!(!create_multiple_entries().is_empty());
    }

    #[test]
    fn multi_sketch_contains_nine() {
        assert_eq!(create_multiple_entries().get_n(), 26);
    }

    #[test]
    fn multi_sketch_serialize() {
        let sketch = create_multiple_entries();
        let serial = sketch.serialize(0).unwrap();
        let deserial = str_deserialize(&serial).unwrap();
        check_approx_equal(&sketch, &deserial);
    }

    #[test]
    fn test_serialize_random() {
        let mut sketch = create_empty();

        for _ in 0..N {
            let r = Alphanumeric.sample_string(&mut rand::rng(), 16);
            sketch.pin_mut().update(&r);
        }

        let serial = sketch.serialize(0).unwrap();
        let deserial = str_deserialize(&serial).unwrap();
        check_approx_equal(&sketch, &deserial);
    }
}

/// Provides a quantiles sketch for the [`Vec<u8>`] byte array type.
#[cxx::bridge(namespace = "rust_sketch")]
#[allow(
    clippy::module_name_repetitions,
    clippy::must_use_candidate,
    clippy::missing_errors_doc
)]
pub mod byte {
    unsafe extern "C++" {
        include!("rust_sketch/src/include/quantiles.hpp");
        /// Rust façade for C++ `datasketches::quantile_sketch<std::vector<uin8_t>>`
        type byte_sketch_t; // trigger instantiation of template quantiles_sketch_derived<byte_array>;

        /// Create a new quantiles sketch.
        ///
        /// This will create a new quantiles sketch object and wrap it in a
        /// [`cxx::UniquePtr`] (a wrapper for [`std::unique_ptr`](https://en.cppreference.com/w/cpp/memory/unique_ptr)).
        /// This ensures that ownership rules are enforced (on both Rust and C++) side and prevent memory leaks.
        /// All interaction must be done through the pointer returned through this function.
        ///
        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for full algorithm details and how the parameters affect behaviour.
        ///
        /// # Example
        /// ```
        /// let sketch = rust_sketch::quantiles::byte::new_byte_sketch(1024);
        /// ```
        ///
        /// # Panics
        /// If memory allocation for the sketch object fails in C++.
        #[rust_name = "new_byte_sketch"]
        fn new_quantiles_sketch(K: u16) -> UniquePtr<byte_sketch_t>;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        fn update(self: Pin<&mut byte_sketch_t>, value: &[u8]);

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        /// # Errors
        /// Rare, but might throw a consistency error if the C++ layer experienced an internal consistency fault.
        fn merge(self: Pin<&mut byte_sketch_t>, other: &byte_sketch_t) -> Result<()>;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        /// # Errors
        /// If called on an empty then an error will be returned.
        fn get_min_item(self: &byte_sketch_t) -> Result<Vec<u8>>;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        /// # Errors
        /// If called on an empty then an error will be returned.
        fn get_max_item(self: &byte_sketch_t) -> Result<Vec<u8>>;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        fn is_empty(self: &byte_sketch_t) -> bool;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        fn get_k(self: &byte_sketch_t) -> u16;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        fn get_n(self: &byte_sketch_t) -> u64;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        fn get_num_retained(self: &byte_sketch_t) -> u32;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        fn is_estimation_mode(self: &byte_sketch_t) -> bool;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        /// # Errors
        /// If called on an empty then an error will be returned.
        fn get_quantile(self: &byte_sketch_t, rank: f64, inclusive: bool) -> Result<Vec<u8>>;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        /// # Errors
        /// If called on an empty then an error will be returned.
        fn get_rank(self: &byte_sketch_t, item: &[u8], inclusive: bool) -> Result<f64>;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        fn get_normalized_rank_error(self: &byte_sketch_t, is_pmf: bool) -> f64;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        fn get_serialized_size_bytes(self: &byte_sketch_t) -> usize;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        /// # Errors
        /// If an error in the serialization stream occurs, then the exception is wrapped in the error message.
        fn serialize(self: &byte_sketch_t, header_size_bytes: u32) -> Result<Vec<u8>>;

        /// Please see [quantiles sketch](https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp)
        /// for documentation.
        ///
        /// # Errors
        /// If an error in the deserialization stream occurs, then the exception is wrapped in the error message.
        #[rust_name = "byte_deserialize"]
        fn deserialize(bytes: &[u8]) -> Result<UniquePtr<byte_sketch_t>>;
    }
}

unsafe impl Send for byte::byte_sketch_t {}

#[cfg(test)]
mod byte_tests {

    const N: i32 = 100_000;
    const ALLOWED_F64_EPSILON: f64 = 0.01;

    use super::{
        byte::{byte_deserialize, byte_sketch_t},
        *,
    };
    use cxx::UniquePtr;
    use rand::distr::{Alphanumeric, SampleString};

    // convenience alias
    use byte_sketch_t as sketch_type;

    fn create_empty() -> UniquePtr<sketch_type> {
        byte::new_byte_sketch(1024)
    }

    fn create_single_entry() -> UniquePtr<sketch_type> {
        let mut sketch = create_empty();
        sketch.pin_mut().update("hello".as_bytes());
        sketch
    }

    fn create_multiple_entries() -> UniquePtr<sketch_type> {
        let mut sketch = create_empty();
        for i in 'a'..='z' {
            sketch.pin_mut().update(&[i as u8]);
        }
        sketch
    }

    fn check_approx_equal(lhs: &UniquePtr<sketch_type>, rhs: &UniquePtr<sketch_type>) {
        assert_eq!(lhs.is_empty(), rhs.is_empty());
        assert_eq!(lhs.get_k(), rhs.get_k());
        assert_eq!(lhs.get_n(), rhs.get_n());
        assert!(
            (lhs.get_normalized_rank_error(true) - rhs.get_normalized_rank_error(true)).abs()
                < ALLOWED_F64_EPSILON
        );
        assert_eq!(lhs.get_num_retained(), rhs.get_num_retained());

        if !lhs.is_empty() && !rhs.is_empty() {
            assert_eq!(lhs.get_min_item().unwrap(), rhs.get_min_item().unwrap());
            assert_eq!(lhs.get_max_item().unwrap(), rhs.get_max_item().unwrap());
            let mut f = 0.0;
            while f < 1.0 {
                assert_eq!(
                    lhs.get_quantile(f, true).unwrap(),
                    rhs.get_quantile(f, true).unwrap()
                );
                f += 0.1;
            }
        }
    }

    #[test]
    fn creates() {
        create_empty();
    }

    #[test]
    fn empty_sketch_returns_errors() {
        let sketch = create_empty();
        assert!(sketch.get_min_item().is_err());
        assert!(sketch.get_max_item().is_err());
        assert!(sketch.get_quantile(0.0, true).is_err());
        assert!(sketch.get_rank(&[], true).is_err());
    }

    #[test]
    fn empty_sketch_is_empty() {
        assert!(create_empty().is_empty());
    }

    #[test]
    fn empty_sketch_contains_zero() {
        assert_eq!(create_empty().get_n(), 0);
    }

    #[test]
    fn empty_sketch_serialize() {
        let sketch = create_empty();
        let serial = sketch.serialize(0).unwrap();
        let deserial = byte_deserialize(&serial).unwrap();
        check_approx_equal(&sketch, &deserial);
    }

    #[test]
    fn single_sketch_returns_min() {
        let sketch = create_single_entry();
        assert_eq!(sketch.get_min_item().unwrap(), "hello".as_bytes());
    }

    #[test]
    fn single_sketch_returns_max() {
        let sketch = create_single_entry();
        assert_eq!(sketch.get_max_item().unwrap(), "hello".as_bytes());
    }

    #[test]
    fn single_sketch_returns_quantile() {
        let sketch = create_single_entry();
        assert_eq!(sketch.get_quantile(0.5, true).unwrap(), "hello".as_bytes());
    }

    #[test]
    fn single_sketch_returns_rank() {
        let sketch = create_single_entry();
        assert!(
            (sketch.get_rank("hello".as_bytes(), true).unwrap() - 1.0).abs() < ALLOWED_F64_EPSILON
        );
    }

    #[test]
    fn single_sketch_not_empty() {
        assert!(!create_single_entry().is_empty());
    }

    #[test]
    fn single_sketch_contains_one() {
        assert_eq!(create_single_entry().get_n(), 1);
    }

    #[test]
    fn single_sketch_serialize() {
        let sketch = create_single_entry();
        let serial = sketch.serialize(0).unwrap();
        let deserial = byte_deserialize(&serial).unwrap();
        check_approx_equal(&sketch, &deserial);
    }

    #[test]
    fn empty_merge_stays_empty() {
        let mut sketch_l = create_empty();
        let sketch_r = create_empty();
        sketch_l.pin_mut().merge(&sketch_r).unwrap();
        assert!(sketch_l.is_empty());
    }

    #[test]
    fn empty_merge_single_is_single() {
        let mut sketch_l = create_empty();
        let sketch_r = create_single_entry();
        sketch_l.pin_mut().merge(&sketch_r).unwrap();
        assert_eq!(sketch_l.get_n(), 1);
        check_approx_equal(&sketch_l, &sketch_r);
    }

    #[test]
    fn single_merge_empty_is_single() {
        let mut sketch_l = create_single_entry();
        let sketch_r = create_empty();
        sketch_l.pin_mut().merge(&sketch_r).unwrap();
        assert_eq!(sketch_l.get_n(), 1);
        assert_eq!(
            sketch_l.get_min_item().unwrap(),
            sketch_l.get_max_item().unwrap()
        );
        assert_eq!(sketch_l.get_num_retained(), 1);
    }

    #[test]
    fn multi_sketch_merge() {
        let mut sketch_l = create_multiple_entries();
        let sketch_r = create_multiple_entries();
        sketch_l.pin_mut().merge(&sketch_r).unwrap();
        assert_eq!(sketch_l.get_n(), sketch_r.get_n() * 2);

        let mut expected = create_empty();
        for i in 'a'..='z' {
            expected.pin_mut().update(&[i as u8]);
            expected.pin_mut().update(&[i as u8]);
        }
        check_approx_equal(&sketch_l, &expected);
    }

    #[test]
    fn multi_sketch_returns_min() {
        let sketch = create_multiple_entries();
        assert_eq!(sketch.get_min_item().unwrap(), "a".as_bytes());
    }

    #[test]
    fn multi_sketch_returns_max() {
        let sketch = create_multiple_entries();
        assert_eq!(sketch.get_max_item().unwrap(), "z".as_bytes());
    }

    #[test]
    fn multi_sketch_returns_quantile() {
        let sketch = create_multiple_entries();
        assert_eq!(sketch.get_quantile(0.5, true).unwrap(), "m".as_bytes());
    }

    #[test]
    fn multi_sketch_returns_rank() {
        let sketch = create_multiple_entries();
        assert!(
            (sketch.get_rank("a".as_bytes(), true).unwrap() - 0.04).abs() < ALLOWED_F64_EPSILON
        );
    }

    #[test]
    fn multi_sketch_not_empty() {
        assert!(!create_multiple_entries().is_empty());
    }

    #[test]
    fn multi_sketch_contains_nine() {
        assert_eq!(create_multiple_entries().get_n(), 26);
    }

    #[test]
    fn multi_sketch_serialize() {
        let sketch = create_multiple_entries();
        let serial = sketch.serialize(0).unwrap();
        let deserial = byte_deserialize(&serial).unwrap();
        check_approx_equal(&sketch, &deserial);
    }

    #[test]
    fn test_serialize_random() {
        let mut sketch = create_empty();

        for _ in 0..N {
            let r = Alphanumeric.sample_string(&mut rand::rng(), 16);
            sketch.pin_mut().update(r.as_bytes());
        }

        let serial = sketch.serialize(0).unwrap();
        let deserial = byte_deserialize(&serial).unwrap();
        check_approx_equal(&sketch, &deserial);
    }
}
