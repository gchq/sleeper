/// A user defined function for `DataFusion` to create quantile sketches
/// of data as it is being compacted.
///
/// This function is designed to be used with a array of columns, one per Sleeper
/// row key field. The return value is just the first column, untransformed. The sketches
/// are produced as a side effect.
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
use std::{
    any::Any,
    fmt::Debug,
    iter::zip,
    sync::{Mutex, MutexGuard},
};

use arrow::{
    array::AsArray,
    datatypes::{
        BinaryType, DataType, Int32Type, Int64Type, LargeBinaryType, LargeUtf8Type, Utf8Type,
    },
};
use datafusion::{
    common::{internal_err, DFSchema, Result},
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility},
    scalar::ScalarValue,
};

use super::sketch::{update_sketch, DataSketchVariant, K};

pub(crate) struct SketchUDF {
    signature: Signature,
    invoke_count: Mutex<usize>,
    sketch: Mutex<Vec<DataSketchVariant>>,
}

impl Debug for SketchUDF {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SketchUDF")
            .field("signature", &self.signature)
            .field("invoke_count", &self.invoke_count)
            .field("sketch", &self.sketch)
            .finish()
    }
}

impl SketchUDF {
    /// Create a new sketch function based on the schema of the row key fields.
    ///
    pub fn new(schema: &DFSchema, row_keys: &[String]) -> Self {
        Self {
            signature: Signature::exact(get_row_key_types(schema, row_keys), Volatility::Immutable),
            invoke_count: Mutex::default(),
            sketch: Mutex::new(make_sketches_for_schema(schema, row_keys)),
        }
    }

    pub fn get_sketch(&self) -> MutexGuard<'_, Vec<DataSketchVariant>> {
        self.sketch.lock().unwrap()
    }

    pub fn get_invoke_count(&self) -> usize {
        *self.invoke_count.lock().unwrap()
    }
}

/// Create a [`Vec`] of data types for this schema from the row keys.
///
/// # Panics
/// If a row key field can't be found in the schema.
fn get_row_key_types(schema: &DFSchema, row_keys: &[String]) -> Vec<DataType> {
    row_keys
        .iter()
        .map(|name| {
            schema
                .field_with_unqualified_name(name)
                .unwrap()
                .data_type()
                .to_owned()
        })
        .collect()
}

/// Create a vector of Data Sketches.
///
/// This creates the appropriate data sketch implementations based on on the row key fields
/// and the data types in the schema. Each type is wrapped in a [`DataSketchVariant`] variant type.
///
/// # Panics
/// If a row key field can't be found in the schema.
///
fn make_sketches_for_schema(
    schema: &DFSchema,
    row_key_fields: &[String],
) -> Vec<DataSketchVariant> {
    row_key_fields
        .iter()
        .map(|name| {
            DataSketchVariant::new(
                schema
                    .field_with_unqualified_name(name)
                    .unwrap()
                    .data_type(),
                K,
            )
        })
        .collect()
}

impl ScalarUDFImpl for SketchUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "sketch"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        // Return type will be type of first row key column
        Ok(args[0].clone())
    }

    fn invoke(&self, columns: &[ColumnarValue]) -> Result<ColumnarValue> {
        *self.invoke_count.lock().unwrap() += 1;

        let mut sk_lock = self.sketch.lock().unwrap();

        for (sketch, col) in zip(sk_lock.iter_mut(), columns) {
            match col {
                ColumnarValue::Array(array) => {
                    // dynamic dispatch. Match the datatype to the type of sketch to update.
                    match array.data_type() {
                        DataType::Int32 => update_sketch(sketch, &array.as_primitive::<Int32Type>()),
                        DataType::Int64 => update_sketch(sketch, &array.as_primitive::<Int64Type>()),
                        DataType::Utf8 => update_sketch(
                            sketch,
                            &array.as_string::<<Utf8Type as arrow::datatypes::ByteArrayType>::Offset>(),
                        ),
                        DataType::LargeUtf8 => update_sketch(
                            sketch,
                            &array.as_string::<<LargeUtf8Type as arrow::datatypes::ByteArrayType>::Offset>(),
                        ),
                        DataType::Utf8View => update_sketch(
                            sketch,
                            &array.as_string_view(),
                        ),
                        DataType::Binary => update_sketch(
                            sketch,
                            &array.as_binary::<<BinaryType as arrow::datatypes::ByteArrayType>::Offset>(),
                        ),
                        DataType::LargeBinary => update_sketch(
                            sketch,
                            &array.as_binary::<<LargeBinaryType as arrow::datatypes::ByteArrayType>::Offset>(),
                        ),
                        DataType::BinaryView => update_sketch(
                            sketch,
                            &array.as_binary_view(),
                        ),
                        _ => return internal_err!("Row type {} not supported for Sleeper row key field", array.data_type()),
                    }
                }

                ColumnarValue::Scalar(
                    ScalarValue::Utf8(Some(value))
                    | ScalarValue::LargeUtf8(Some(value))
                    | ScalarValue::Utf8View(Some(value)),
                ) => {
                    sketch.update(value);
                }
                ColumnarValue::Scalar(
                    ScalarValue::Binary(Some(value))
                    | ScalarValue::LargeBinary(Some(value))
                    | ScalarValue::BinaryView(Some(value)),
                ) => {
                    sketch.update(value);
                }
                ColumnarValue::Scalar(ScalarValue::Int32(Some(value))) => {
                    sketch.update(value);
                }
                ColumnarValue::Scalar(ScalarValue::Int64(Some(value))) => {
                    sketch.update(value);
                }
                x @ ColumnarValue::Scalar(_) => {
                    return internal_err!(
                        "Row type {} not supported for Sleeper row key field",
                        x.data_type()
                    )
                }
            }
        }

        Ok(columns[0].clone())
    }
}
