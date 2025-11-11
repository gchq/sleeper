//! This module provides some extra required wrappers around sketches
//! functionality such as common traits.
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
use crate::datafusion::sketch_udf::SketchUDF;
use arrow::{array::ArrayAccessor, datatypes::DataType};
use bytes::{Buf, BufMut};
use color_eyre::eyre::eyre;
use cxx::{Exception, UniquePtr};
use datafusion::{
    common::DFSchema,
    dataframe::DataFrame,
    error::DataFusionError,
    logical_expr::{ScalarUDF, ident},
    parquet::data_type::AsBytes,
};
use log::info;
use num_format::{Locale, ToFormattedString};
use objectstore_ext::s3::ObjectStoreFactory;
use rust_sketch::quantiles::{
    byte::{byte_deserialize, byte_sketch_t, new_byte_sketch},
    i64::{i64_deserialize, i64_sketch_t, new_i64_sketch},
    str::{new_str_sketch, str_deserialize, string_sketch_t},
};
use std::{
    fmt::Debug,
    io::Write,
    mem::size_of,
    path::PathBuf,
    sync::{Arc, Mutex},
};
use url::Url;

/// Constant size for quantiles data sketches
pub const K: u16 = 1024;

pub enum DataSketchVariant {
    Int(DataType, UniquePtr<i64_sketch_t>),
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
        Some((*self).into())
    }

    fn to_str(&self) -> Option<&str> {
        None
    }

    fn to_bytes(&self) -> Option<&[u8]> {
        None
    }
}

impl Item for i64 {
    #[allow(clippy::cast_possible_truncation)]
    fn to_i32(&self) -> Option<i32> {
        Some(*self as i32)
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
    /// If the value provided cannot be converted to the type of this `DataSketch`, i.e.
    /// if you try to update an i32 sketch with a string.
    pub fn update<T>(&mut self, value: &T)
    where
        T: Item,
    {
        match self {
            DataSketchVariant::Int(_, s) => s.pin_mut().update(value.to_i64().unwrap()),
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
            DataSketchVariant::Int(_, s) => s.get_k(),
            DataSketchVariant::Str(_, s) => s.get_k(),
            DataSketchVariant::Bytes(_, s) => s.get_k(),
        }
    }

    /// Gets the total number of items in this sketch.
    ///
    /// Note that as this sketches are approximate, only a fraction of this amount
    /// is retained by the sketch. Please see [`DataSketchVariant::get_num_retained`].
    ///
    /// Please see Apache data sketch C++ documentation for full explanation.
    #[must_use]
    pub fn get_n(&self) -> u64 {
        match self {
            DataSketchVariant::Int(_, s) => s.get_n(),
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
            DataSketchVariant::Int(_, s) => s.get_num_retained(),
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
            DataSketchVariant::Int(_, s) => Ok(s.get_min_item().map(Box::new)?),
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
            DataSketchVariant::Int(_, s) => Ok(s.get_max_item().map(Box::new)?),
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
            DataSketchVariant::Int(_, s) => s.serialize(header_size_bytes),
            DataSketchVariant::Str(_, s) => s.serialize(header_size_bytes),
            DataSketchVariant::Bytes(_, s) => s.serialize(header_size_bytes),
        }
    }

    /// Return the underlying Arrow [`DataType`] this sketch is intended for.
    #[must_use]
    #[allow(dead_code)]
    pub fn data_type(&self) -> DataType {
        match self {
            DataSketchVariant::Int(t, _)
            | DataSketchVariant::Str(t, _)
            | DataSketchVariant::Bytes(t, _) => t.clone(),
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
            t @ (DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64) => {
                DataSketchVariant::Int(t.clone(), new_i64_sketch(k))
            }
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

/// A sketching manager to handle creation of sketching functions.
///
/// This can create a User Defined Sketch function for `DataFusion` as
/// and modify a [`DataFrame`] to add sketch generation to it.
#[derive(Debug)]
pub struct Sketcher<'a> {
    /// Row key names
    row_keys: &'a Vec<String>,
    /// A created sketch function.
    sketch: Arc<ScalarUDF>,
}

impl<'a> Sketcher<'a> {
    pub fn new(row_keys: &'a Vec<String>, schema: &DFSchema) -> Sketcher<'a> {
        Sketcher {
            row_keys,
            sketch: Arc::new(ScalarUDF::from(SketchUDF::new(
                schema,
                &row_keys.iter().map(String::as_str).collect::<Vec<_>>(),
            ))),
        }
    }

    // The created sketch UDF.
    pub fn sketch(&self) -> &Arc<ScalarUDF> {
        &self.sketch
    }

    /// Adds quantile sketch creation to the given frame.
    ///
    /// An extra SELECT stage is added to the frame's plan. The created sketch
    /// function is also returned so sketches can be accessed after execution
    /// of the frame's plan.
    pub fn apply_sketch(&self, frame: DataFrame) -> Result<DataFrame, DataFusionError> {
        // Extract all column names
        let column_bind = frame.schema().columns();
        let col_names = column_bind
            .iter()
            .map(datafusion::common::Column::name)
            .collect::<Vec<_>>();

        let row_key_exprs = self.row_keys.iter().map(ident).collect::<Vec<_>>();

        // Iterate through column names, mapping each into an `Expr`
        let mut col_names_expr = Vec::new();
        for col_name in col_names {
            // Have we found the first row key field?
            let expr = if self.row_keys[0] == *col_name {
                // Sketch function needs to be called with each row key field
                self.sketch.call(row_key_exprs.clone()).alias(col_name)
            } else {
                ident(col_name)
            };
            col_names_expr.push(expr);
        }
        frame.select(col_names_expr)
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
pub async fn serialise_sketches(
    store_factory: &ObjectStoreFactory,
    path: &Url,
    sketches_mutex: &Mutex<Vec<DataSketchVariant>>,
) -> color_eyre::Result<()> {
    let mut buf = vec![].writer();

    let mut size = 0;
    let sketches_len;
    {
        let sketches = &*sketches_mutex.lock().unwrap();
        sketches_len = sketches.len();
        // for each sketch write the size i32, followed by bytes
        for sketch in sketches {
            let serialised = sketch.serialize(0)?;
            buf.write_all(&(serialised.len() as u32).to_be_bytes())?;
            buf.write_all(&serialised)?;
            size += serialised.len() + size_of::<u32>();
        }
    }

    //Path part of S3 URL
    let store_path = object_store::path::Path::from(path.path());

    // Save to object store
    let store = store_factory.get_object_store(path)?;

    store.put(&store_path, buf.into_inner().into()).await?;

    info!(
        "Serialised {} ({} bytes) sketches to {}",
        sketches_len,
        size.to_formatted_string(&Locale::en),
        path
    );
    Ok(())
}

/// Extract the Data Sketch result and write it out.
///
/// This function should be called after a `DataFusion` operation has completed. The sketch function will be asked for
/// the current sketch.
///
/// # Errors
/// If the sketch couldn't be serialised.
pub async fn output_sketch(
    store_factory: &ObjectStoreFactory,
    output_path: &Url,
    sketch_func: &Arc<ScalarUDF>,
) -> Result<(), DataFusionError> {
    let binding = sketch_func.inner();
    let inner_function: Option<&SketchUDF> = binding.as_any().downcast_ref();
    if let Some(func) = inner_function {
        {
            // Limit scope of MutexGuard
            let first_sketch = &func.get_sketch().lock().unwrap()[0];
            info!(
                "Made {} calls to sketch UDF. Quantile sketch column 0 retained {} out of {} values (K value = {}).",
                func.get_invoke_count().to_formatted_string(&Locale::en),
                first_sketch
                    .get_num_retained()
                    .to_formatted_string(&Locale::en),
                first_sketch.get_n().to_formatted_string(&Locale::en),
                first_sketch.get_k().to_formatted_string(&Locale::en)
            );
        }

        // Serialise the sketch
        serialise_sketches(
            store_factory,
            &create_sketch_path(output_path),
            func.get_sketch(),
        )
        .await
        .map_err(|e| DataFusionError::External(e.into()))?;
    }
    Ok(())
}

/// Creates a file path suitable for writing sketches to.
///
#[must_use]
pub fn create_sketch_path(output_path: &Url) -> Url {
    let mut res = output_path.clone();
    res.set_path(
        &PathBuf::from(output_path.path())
            .with_extension("sketches")
            .to_string_lossy(),
    );
    res
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
