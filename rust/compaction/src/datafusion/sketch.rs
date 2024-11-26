//! This module provides some extra required wrappers around sketches
//! functionality such as common traits.
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
use crate::aws_s3::ObjectStoreFactory;
use arrow::array::ArrayAccessor;
use arrow::datatypes::DataType;
use bytes::{Buf, BufMut, Bytes};
use color_eyre::eyre::eyre;
use cxx::{Exception, UniquePtr};
use datafusion::parquet::data_type::AsBytes;
use log::info;
use num_format::{Locale, ToFormattedString};
use rust_sketch::quantiles::byte::{byte_deserialize, byte_sketch_t, new_byte_sketch};
use rust_sketch::quantiles::i32::{i32_deserialize, i32_sketch_t, new_i32_sketch};
use rust_sketch::quantiles::i64::{i64_deserialize, i64_sketch_t, new_i64_sketch};
use rust_sketch::quantiles::str::{new_str_sketch, str_deserialize, string_sketch_t};
use std::fmt::Debug;
use std::io::Write;
use std::mem::size_of;
use url::Url;

/// Constant size for quantiles data sketches
pub const K: u16 = 1024;

pub enum DataSketchVariant {
    I32(UniquePtr<i32_sketch_t>),
    I64(UniquePtr<i64_sketch_t>),
    Str(DataType, UniquePtr<string_sketch_t>),
    Bytes(DataType, UniquePtr<byte_sketch_t>),
}

impl Debug for DataSketchVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple(std::any::type_name::<Self>()).finish()
    }
}

pub trait Item {
    fn to_i32(&self) -> Option<i32>;
    fn to_i64(&self) -> Option<i64>;
    fn to_str(&self) -> Option<&str>;
    fn to_bytes(&self) -> Option<&[u8]>;
}

impl Item for i32 {
    fn to_i32(&self) -> Option<i32> {
        Some(*self)
    }

    fn to_i64(&self) -> Option<i64> {
        None
    }

    fn to_str(&self) -> Option<&str> {
        None
    }

    fn to_bytes(&self) -> Option<&[u8]> {
        None
    }
}

impl Item for i64 {
    fn to_i32(&self) -> Option<i32> {
        None
    }

    fn to_i64(&self) -> Option<i64> {
        Some(*self)
    }

    fn to_str(&self) -> Option<&str> {
        None
    }

    fn to_bytes(&self) -> Option<&[u8]> {
        None
    }
}

impl Item for String {
    fn to_i32(&self) -> Option<i32> {
        None
    }

    fn to_i64(&self) -> Option<i64> {
        None
    }

    fn to_str(&self) -> Option<&str> {
        Some(self.as_ref())
    }

    fn to_bytes(&self) -> Option<&[u8]> {
        None
    }
}

impl Item for &str {
    fn to_i32(&self) -> Option<i32> {
        None
    }

    fn to_i64(&self) -> Option<i64> {
        None
    }

    fn to_str(&self) -> Option<&str> {
        Some(self)
    }

    fn to_bytes(&self) -> Option<&[u8]> {
        None
    }
}

impl Item for Vec<u8> {
    fn to_i32(&self) -> Option<i32> {
        None
    }

    fn to_i64(&self) -> Option<i64> {
        None
    }

    fn to_str(&self) -> Option<&str> {
        None
    }

    fn to_bytes(&self) -> Option<&[u8]> {
        Some(self.as_ref())
    }
}

impl Item for &[u8] {
    fn to_i32(&self) -> Option<i32> {
        None
    }

    fn to_i64(&self) -> Option<i64> {
        None
    }

    fn to_str(&self) -> Option<&str> {
        None
    }

    fn to_bytes(&self) -> Option<&[u8]> {
        Some(self)
    }
}

impl DataSketchVariant {
    /// Updates this sketch variant with the given value.
    ///
    /// Calls the update method of the underlying sketch.
    ///
    /// # Panics
    /// If the value provided cannot be converted to the type of this [`DataSketch`], i.e.
    /// if you try to update an i32 sketch with a string.
    pub fn update<T>(&mut self, value: &T)
    where
        T: Item,
    {
        match self {
            DataSketchVariant::I32(s) => s.pin_mut().update(value.to_i32().unwrap()),
            DataSketchVariant::I64(s) => s.pin_mut().update(value.to_i64().unwrap()),
            DataSketchVariant::Str(_, s) => s.pin_mut().update(value.to_str().unwrap()),
            DataSketchVariant::Bytes(_, s) => s.pin_mut().update(value.to_bytes().unwrap()),
        }
    }

    /// Gets the 'k' parameter of the quantile sketch.
    ///
    /// Please see Apache data sketch C++ documentation for full explanation.
    #[must_use]
    pub fn get_k(&self) -> u16 {
        match self {
            DataSketchVariant::I32(s) => s.get_k(),
            DataSketchVariant::I64(s) => s.get_k(),
            DataSketchVariant::Str(_, s) => s.get_k(),
            DataSketchVariant::Bytes(_, s) => s.get_k(),
        }
    }

    /// Gets the total number of items in this sketch.
    ///
    /// Note that as this sketches are approximate, only a fraction of this amount
    /// is retained by the sketch. Please see [`get_num_retained`].
    ///
    /// Please see Apache data sketch C++ documentation for full explanation.
    #[must_use]
    pub fn get_n(&self) -> u64 {
        match self {
            DataSketchVariant::I32(s) => s.get_n(),
            DataSketchVariant::I64(s) => s.get_n(),
            DataSketchVariant::Str(_, s) => s.get_n(),
            DataSketchVariant::Bytes(_, s) => s.get_n(),
        }
    }

    /// Gets the number of individual items retained by the sketch.
    ///
    /// Please see Apache data sketch C++ documentation for full explanation.
    #[must_use]
    pub fn get_num_retained(&self) -> u32 {
        match self {
            DataSketchVariant::I32(s) => s.get_num_retained(),
            DataSketchVariant::I64(s) => s.get_num_retained(),
            DataSketchVariant::Str(_, s) => s.get_num_retained(),
            DataSketchVariant::Bytes(_, s) => s.get_num_retained(),
        }
    }

    /// Get the minimum item from this sketch.
    ///
    /// # Errors
    /// If the sketch is empty an error is thrown.
    #[allow(dead_code)]
    pub fn get_min_item(&self) -> Result<Box<dyn Item>, Exception> {
        match self {
            DataSketchVariant::I32(s) => Ok(s.get_min_item().map(Box::new)?),
            DataSketchVariant::I64(s) => Ok(s.get_min_item().map(Box::new)?),
            DataSketchVariant::Str(_, s) => Ok(s.get_min_item().map(Box::new)?),
            DataSketchVariant::Bytes(_, s) => Ok(s.get_min_item().map(Box::new)?),
        }
    }

    /// Get the maximum item from this sketch.
    ///
    /// # Errors
    /// If the sketch is empty an error is thrown.
    #[allow(dead_code)]
    pub fn get_max_item(&self) -> Result<Box<dyn Item>, Exception> {
        match self {
            DataSketchVariant::I32(s) => Ok(s.get_max_item().map(Box::new)?),
            DataSketchVariant::I64(s) => Ok(s.get_max_item().map(Box::new)?),
            DataSketchVariant::Str(_, s) => Ok(s.get_max_item().map(Box::new)?),
            DataSketchVariant::Bytes(_, s) => Ok(s.get_max_item().map(Box::new)?),
        }
    }

    /// Serialise this data sketch to bytes.
    ///
    /// This serialisation is compatible with the Java equivalent.
    ///
    /// # Errors
    /// If the sketch couldn't be serialised at the C++ layer.
    ///
    pub fn serialize(&self, header_size_bytes: u32) -> Result<Vec<u8>, Exception> {
        match self {
            DataSketchVariant::I32(s) => s.serialize(header_size_bytes),
            DataSketchVariant::I64(s) => s.serialize(header_size_bytes),
            DataSketchVariant::Str(_, s) => s.serialize(header_size_bytes),
            DataSketchVariant::Bytes(_, s) => s.serialize(header_size_bytes),
        }
    }

    /// Return the underlying Arrow [`DataType`] this sketch is intended for.
    #[must_use]
    #[allow(dead_code)]
    pub fn data_type(&self) -> DataType {
        match self {
            DataSketchVariant::I32(_) => DataType::Int32,
            DataSketchVariant::I64(_) => DataType::Int64,
            DataSketchVariant::Str(t, _) | DataSketchVariant::Bytes(t, _) => t.clone(),
        }
    }

    /// Create a new sketch for the given Arrow [`DataType`].
    ///
    /// The `k` parameter is passed to the underlying datasketch. See
    /// Apache Data Sketch documentation for an explanation.
    ///
    /// # Panics
    /// If the given [`DataType`] is not supported.
    #[must_use]
    pub fn new(d: &DataType, k: u16) -> DataSketchVariant {
        match d {
            DataType::Int32 => DataSketchVariant::I32(new_i32_sketch(k)),
            DataType::Int64 => DataSketchVariant::I64(new_i64_sketch(k)),
            t @ (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => {
                DataSketchVariant::Str(t.clone(), new_str_sketch(k))
            }
            t @ (DataType::Binary | DataType::LargeBinary | DataType::BinaryView) => {
                DataSketchVariant::Bytes(t.clone(), new_byte_sketch(k))
            }
            _ => {
                panic!("DataType not supported {d}");
            }
        }
    }
}

/// Write the data sketches to the named file.
///
/// For each sketch, the length of the serialised sketch is written in
/// big endian format, compatible with [`DataOutputStream::writeInt`](https://docs.oracle.com/en/java/javase/20/docs/api/java.base/java/io/DataOutputStream.html#writeInt(int))
/// followed by the bytes of the sketch.
///
/// # Errors
/// The data sketch serialisation might also throw errors from the underlying
/// data sketch library.
#[allow(clippy::cast_possible_truncation)]
pub fn serialise_sketches(
    store_factory: &ObjectStoreFactory,
    path: &Url,
    sketches: &[DataSketchVariant],
) -> color_eyre::Result<()> {
    let mut buf = vec![].writer();

    let mut size = 0;
    // for each sketch write the size i32, followed by bytes
    for sketch in sketches {
        let serialised = sketch.serialize(0)?;
        buf.write_all(&(serialised.len() as u32).to_be_bytes())?;
        buf.write_all(&serialised)?;
        size += serialised.len() + size_of::<u32>();
    }

    //Path part of S3 URL
    let store_path = object_store::path::Path::from(path.path());

    // Save to object store
    let store = store_factory.get_object_store(path)?;

    futures::executor::block_on(store.put(&store_path, buf.into_inner().into()))?;

    info!(
        "Serialised {} ({} bytes) sketches to {}",
        sketches.len(),
        size.to_formatted_string(&Locale::en),
        path
    );
    Ok(())
}

#[allow(clippy::missing_errors_doc)]
pub fn deserialise_sketches(
    path: &Url,
    key_types: Vec<DataType>,
) -> color_eyre::Result<Vec<DataSketchVariant>> {
    let factory = ObjectStoreFactory::new(None);
    deserialise_sketches_with_factory(&factory, path, key_types)
}

fn deserialise_sketches_with_factory(
    store_factory: &ObjectStoreFactory,
    path: &Url,
    key_types: Vec<DataType>,
) -> color_eyre::Result<Vec<DataSketchVariant>> {
    let store_path = object_store::path::Path::from(path.path());
    let store = store_factory.get_object_store(path)?;
    let result = futures::executor::block_on(store.get(&store_path))?;
    read_sketches_from_result(result, key_types)
}

fn read_sketches_from_result(
    result: object_store::GetResult,
    key_types: Vec<DataType>,
) -> color_eyre::Result<Vec<DataSketchVariant>> {
    let mut bytes = futures::executor::block_on(result.bytes())?;
    let mut sketches: Vec<DataSketchVariant> = vec![];
    for key_type in key_types {
        let length = bytes.get_u32() as usize;
        let sketch_bytes = bytes.split_to(length);
        sketches.push(read_sketch(sketch_bytes.as_bytes(), key_type)?);
    }
    Ok(sketches)
}

fn read_sketch(bytes: &[u8], key_type: DataType) -> color_eyre::Result<DataSketchVariant> {
    match key_type {
        DataType::Int32 => Ok(DataSketchVariant::I32(i32_deserialize(bytes)?)),
        DataType::Int64 => Ok(DataSketchVariant::I64(i64_deserialize(bytes)?)),
        t @ (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => {
            Ok(DataSketchVariant::Str(t.clone(), str_deserialize(bytes)?))
        }
        t @ (DataType::Binary | DataType::LargeBinary | DataType::BinaryView) => Ok(
            DataSketchVariant::Bytes(t.clone(), byte_deserialize(bytes)?),
        ),
        _ => Err(eyre!("DataType not supported {key_type}")),
    }
}

/// Update the given sketch from an array.
///
/// The list of values in the given array are updated into the data sketch.
///
/// # Panics
/// Panic if sketch type is not compatible with the item type of the array.
#[allow(clippy::module_name_repetitions)]
pub fn update_sketch<T: Item, A: ArrayAccessor<Item = T>>(
    sketch: &mut DataSketchVariant,
    array: &A,
) {
    for i in 0..array.len() {
        unsafe {
            sketch.update(&array.value_unchecked(i));
        }
    }
}
