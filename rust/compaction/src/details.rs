//! The `internal` module contains the internal functionality and error conditions
//! to actually implement the compaction library.
/*
 * Copyright 2022-2023 Crown Copyright
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
use crate::{
    aws_s3::ObjectStoreFactory,
    sketch::{make_sketches_for_schema, serialise_sketches, update_sketches},
};
use arrow::{
    array::{ArrayAccessor, AsArray},
    datatypes::{
        BinaryType, DataType, Int32Type, Int64Type, LargeBinaryType, LargeUtf8Type, Schema,
        Utf8Type,
    },
    error::ArrowError,
    record_batch::RecordBatch,
    row::{OwnedRow, RowConverter, Rows, SortField},
};
use aws_credential_types::Credentials;
use aws_types::region::Region;

use futures::{executor::BlockingStream, future};
use itertools::{kmerge, Itertools};
use log::{debug, info};
use num_format::{Locale, ToFormattedString};
use parquet::{
    arrow::{
        async_reader::{ParquetObjectReader, ParquetRecordBatchStream},
        AsyncArrowWriter, ParquetRecordBatchStreamBuilder,
    },
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use std::{cell::RefCell, fmt::Display, path::PathBuf, sync::Arc};
use url::Url;

/// A simple iterator for a batch of rows (owned).
#[derive(Debug)]
struct OwnedRowIter {
    rows: Result<Rows, ArrowError>,
    error_reported: bool,
    index: usize,
}

impl OwnedRowIter {
    /// Create a new iterator over some rows.
    fn new(rows: Result<Rows, ArrowError>) -> Self {
        Self {
            rows,
            error_reported: false,
            index: 0,
        }
    }
}

/// Contains compaction results.
///
/// This provides the details of compaction results that Sleeper
/// will use to update its record keeping.
///
pub struct CompactionResult {
    /// The minimum key seen in column zero.
    pub min_key: Vec<u8>,
    /// The maximum key seen in column zero.
    pub max_key: Vec<u8>,
    /// The total number of rows read by a compaction.
    pub rows_read: usize,
    /// The total number of rows written by a compaction.
    pub rows_written: usize,
}

impl Iterator for OwnedRowIter {
    type Item = Result<OwnedRow, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match &self.rows {
            Ok(rows_val) => {
                if self.index == rows_val.num_rows() {
                    return None;
                }
                let row = rows_val.row(self.index).owned();
                self.index += 1;
                Some(Ok(row))
            }
            Err(e) => {
                // This is not really the right way to handle errors inside an iterator
                if self.error_reported {
                    None
                } else {
                    self.error_reported = true;
                    Some(Err(ArrowError::IoError(e.to_string())))
                }
            }
        }
    }
}

/// Merges the given Parquet files and reads the schema from the first.
///
/// This function reads the schema from the first file, then calls
/// `merge_sorted_files_with_schema(...)`.
///
/// The `aws_creds` are optional if you are not attempting to read/write files from S3.
///
/// # Examples
/// ```no_run
/// # use arrow::error::ArrowError;
/// # use url::Url;
/// # use aws_types::region::Region;
/// # use crate::compaction::merge_sorted_files;
/// # tokio_test::block_on(async {
/// let result = merge_sorted_files(None, &Region::new("eu-west-2"), &vec![Url::parse("file:///path/to/file1.parquet").unwrap()], &Url::parse("file:///path/to/output").unwrap(), 65535, 1_000_000, vec![0], vec![0]).await;
/// # })
/// ```
///
/// # Errors
/// There must be at least one input file.
///
#[allow(clippy::too_many_arguments)]
pub async fn merge_sorted_files(
    aws_creds: Option<Credentials>,
    region: &Region,
    input_file_paths: &[Url],
    output_file_path: &Url,
    row_group_size: usize,
    max_page_size: usize,
    row_key_fields: impl AsRef<[usize]>,
    sort_columns: impl AsRef<[usize]>,
) -> Result<CompactionResult, ArrowError> {
    // Read the schema from the first file
    if input_file_paths.is_empty() {
        Err(ArrowError::InvalidArgumentError(
            "No input paths supplied".into(),
        ))
    } else {
        // Create our object store factory
        let store_factory = ObjectStoreFactory::new(aws_creds, region);

        // Java tends to use s3a:// URI scheme instead of s3:// so map it here
        let input_file_paths: Vec<Url> = input_file_paths
            .iter()
            .map(|u| {
                let mut t = u.to_owned();
                if t.scheme() == "s3a" {
                    let _ = t.set_scheme("s3");
                }
                t
            })
            .collect();

        // Change output file scheme
        let mut output_file_path = output_file_path.to_owned();
        if output_file_path.scheme() == "s3a" {
            let _ = output_file_path.set_scheme("s3");
        }

        // Read Schema from first file
        let schema: Arc<Schema> = read_schema(&store_factory, &input_file_paths[0])?;

        // validate all files have same schema
        if validate_schemas_same(&store_factory, &input_file_paths, &schema) {
            // Sort the row key column numbers
            let sorted_row_keys: Vec<usize> = row_key_fields
                .as_ref()
                .iter()
                .sorted()
                .map(usize::to_owned)
                .collect();

            // Create a list of column numbers. Sort columns should be first,
            // followed by others in order
            // so if schema has 5 columns [0, 1, 2, 3, 4] and sort_columns is [1,4],
            // then the full list of columns should be [1, 4, 0, 2, 3]
            let complete_sort_columns: Vec<_> = sort_columns
                .as_ref()
                .iter()
                .copied() //Deref the integers
                // then chain to an iterator of [0, schema.len) with sort columns fitered out
                .chain((0..schema.fields().len()).filter(|i| !sort_columns.as_ref().contains(i)))
                .collect();

            debug!("Sort columns {:?}", sort_columns.as_ref());
            info!("Sorted column order {:?}", complete_sort_columns);

            // Create a vec of all schema columns for row conversion
            let fields = schema
                .fields()
                .iter()
                .map(|field| SortField::new(field.data_type().clone()))
                .collect::<Vec<_>>();

            // now re-order this list according to the indexes in complete_sort_columns
            let fields: Vec<_> = complete_sort_columns
                .iter()
                .map(|&i| fields[i].clone())
                .collect();

            let converter = RowConverter::new(fields)?;

            merge_sorted_files_with_schema(
                &store_factory,
                &input_file_paths,
                &output_file_path,
                row_group_size,
                max_page_size,
                sorted_row_keys,
                complete_sort_columns,
                &schema,
                &Arc::new(RefCell::new(converter)),
            )
            .await
        } else {
            Err(ArrowError::SchemaError(
                "Schemas do not match across all input files".into(),
            ))
        }
    }
}

/// Merges sorted Parquet files into a single output file. They must also have the same schema.
///
/// The files MUST be sorted for this function to work. This is not checked!
///
/// The files are read and then k-way merged into a single output Parquet file.
///
/// # Examples
/// ```ignore
/// let result = merge_sorted_files_with_schema( &vec!["file:///path/to/file1.parquet".into(), "file:///path/to/file2.parquet".into()], "file:///path/to/output.parquet".into(), 65535, 1_000_000, my_schema )?;
/// ```
///
/// # Errors
/// The supplied input file paths must have a valid extension: parquet, parq, or pq.
///
#[allow(clippy::too_many_arguments)]
async fn merge_sorted_files_with_schema(
    store_factory: &ObjectStoreFactory,
    file_paths: &[Url],
    output_file_path: &Url,
    row_group_size: usize,
    max_page_size: usize,
    row_key_fields: impl AsRef<[usize]>,
    sort_columns: impl AsRef<[usize]>,
    schema: &Arc<Schema>,
    converter_ptr: &Arc<RefCell<RowConverter>>,
) -> Result<CompactionResult, ArrowError> {
    // Are the row key columns defined and valid?
    if row_key_fields.as_ref().is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "row_key_fields cannot be empty".into(),
        ));
    } else if sort_columns.as_ref().is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "sort_columns cannot be empty".into(),
        ));
    } else if row_key_fields
        .as_ref()
        .iter()
        .any(|&index| index >= schema.fields().len())
    {
        return Err(ArrowError::InvalidArgumentError(
            "row_key_fields contains invalid column number for schema (too high?)".into(),
        ));
    } else if sort_columns
        .as_ref()
        .iter()
        .any(|&index| index >= schema.fields().len())
    {
        return Err(ArrowError::InvalidArgumentError(
            "sort_columns contains invalid column number for schema (too high?)".into(),
        ));
    }

    let file_iters = future::join_all(
        file_paths
            .iter()
            .map(|file_path| get_file_iterator(store_factory, file_path, None)),
    )
    .await
    .into_iter()
    .collect::<Result<Vec<_>, _>>();

    let converter_clone = converter_ptr.clone();

    let batch_iters = file_iters?.into_iter().map(|file_iter| {
        file_iter.flat_map(|batch| match batch {
            Ok(b) => {
                let rows = converter_ptr.borrow_mut().convert_columns(b.columns());
                OwnedRowIter::new(rows)
            }
            Err(e) => OwnedRowIter::new(Err(e.into())),
        })
    });

    merge_sorted_iters_into_parquet_file(
        store_factory,
        batch_iters,
        output_file_path,
        row_group_size,
        max_page_size,
        row_key_fields,
        schema,
        &converter_clone,
    )
    .await
}

/// Takes a group of Arrow record batch iterators and writes the merged
/// output to a Parquet file.
#[allow(clippy::too_many_arguments)]
async fn merge_sorted_iters_into_parquet_file<A, B>(
    store_factory: &ObjectStoreFactory,
    iters: A,
    output_file_path: &Url,
    row_group_size: usize,
    max_page_size: usize,
    row_key_fields: impl AsRef<[usize]>,
    schema: &Arc<Schema>,
    converter_ptr: &Arc<RefCell<RowConverter>>,
) -> Result<CompactionResult, ArrowError>
where
    A: Iterator<Item = B>,
    B: Iterator<Item = Result<OwnedRow, ArrowError>>,
{
    // Create Parquet writer options
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_data_page_size_limit(max_page_size)
        .set_max_row_group_size(row_group_size)
        .build();

    info!("Row_key columns {:?}", row_key_fields.as_ref());

    // Create Object store async writer
    let store = store_factory.get_object_store(output_file_path)?;
    let path = object_store::path::Path::from(output_file_path.path());
    let (_id, store_writer) = store
        .put_multipart(&path)
        .await
        .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

    info!("Writing to {}", output_file_path);

    // Create Parquet writer
    let mut writer =
        AsyncArrowWriter::try_new(store_writer, schema.clone(), 1024 * 1024, Some(props))?;

    // Data sketches quantiles sketches
    let mut sketches = make_sketches_for_schema(schema, &row_key_fields);

    // Container for errors
    let errors: RefCell<Vec<ArrowError>> = RefCell::new(vec![]);

    let error_free =
        iters.map(|iter| iter.filter_map(|r| r.map_err(|e| errors.borrow_mut().push(e)).ok()));

    // Row K-way merge on iterators
    let merged = kmerge(error_free);

    // Need to collect min/max key on column 0. Not sure we can assume col. 0 is a row key column, so it might not have a quantiles sketch
    let mut min_key_zero = None;
    let mut max_key_zero = None;
    let mut rows_written = 0;
    for chunk in &merged.chunks(row_group_size) {
        // Check for errors
        if !errors.borrow().is_empty() {
            // Return first error
            return Err(errors.borrow_mut().remove(0));
        }

        let rows = chunk.collect::<Vec<_>>();
        let rows = rows
            .iter()
            .map(arrow::row::OwnedRow::row)
            .collect::<Vec<_>>();

        rows_written += rows.len();
        info!(
            "Merged {} rows",
            rows_written.to_formatted_string(&Locale::en)
        );

        let cols = converter_ptr.borrow_mut().convert_rows(rows)?;
        let batch = RecordBatch::try_new(schema.clone(), cols)?;

        // Collect min key zero?
        if min_key_zero.is_none() {
            min_key_zero = Some(get_zero_col_item(&batch, 0));
        }

        // Update max key zero
        max_key_zero = Some(get_zero_col_item(&batch, batch.num_rows() - 1));

        // update the data sketches on this batch
        update_sketches(&batch, &mut sketches, &row_key_fields);

        // write some data sketches here
        batch.column(0);

        writer.write(&batch).await?;
    }

    writer.close().await?;

    // serialise the sketches
    let sketch_path = create_sketch_path(output_file_path);
    serialise_sketches(store_factory, &sketch_path, &sketches)?;

    Ok(CompactionResult {
        min_key: min_key_zero.unwrap_or(Vec::new()),
        max_key: max_key_zero.unwrap_or(Vec::new()),
        rows_read: rows_written,
        rows_written,
    })
}

/// Get the item at a given position in column zero as a byte vector.
///
/// The type of array is used to work out how to do the conversion. Display types
/// are converted via [`Display::to_string()`].
///
/// # Panics
/// If the array datatype is not recognised.
///  
fn get_zero_col_item(batch: &RecordBatch, pos: usize) -> Vec<u8> {
    let zero_col = batch.column(0);
    match zero_col.data_type() {
        DataType::Int32 => get_item_bytes(&zero_col.as_primitive::<Int32Type>(), pos),
        DataType::Int64 => get_item_bytes(&zero_col.as_primitive::<Int64Type>(), pos),
        DataType::Utf8 => get_item_bytes(
            &zero_col.as_string::<<Utf8Type as arrow::datatypes::ByteArrayType>::Offset>(),
            pos,
        ),
        DataType::LargeUtf8 => get_item_bytes(
            &zero_col.as_string::<<LargeUtf8Type as arrow::datatypes::ByteArrayType>::Offset>(),
            pos,
        ),
        DataType::Binary => get_binary_bytes(
            &zero_col.as_binary::<<BinaryType as arrow::datatypes::ByteArrayType>::Offset>(),
            pos,
        ),
        DataType::LargeBinary => get_binary_bytes(
            &zero_col.as_binary::<<LargeBinaryType as arrow::datatypes::ByteArrayType>::Offset>(),
            pos,
        ),
        _ => panic!("Row type {} not supported", zero_col.data_type()),
    }
}

/// Retrieves an item from an array type whose item is string convertible.
///
/// We use the [`Display`] trait to convert the item at position `pos` to a string
/// then return the byte representation of that item.
fn get_item_bytes<T: Display, A: ArrayAccessor<Item = T>>(array: &A, pos: usize) -> Vec<u8> {
    array.value(pos).to_string().into_bytes()
}

/// Retrieves an item from a binary byte array.
///
/// As the array type is already a binary slice, we just
/// convert it to an owning vector.
fn get_binary_bytes<'a, A: ArrayAccessor<Item = &'a [u8]>>(array: &A, pos: usize) -> Vec<u8> {
    array.value(pos).to_vec()
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

/// Creates an iterator of Arrow record batches.
///
/// This creates a file reader for a Parquet file returning the Arrow recordbatches.
/// The optional `batch_size` argument allows for setting the maximum size of each batch
/// returned in one go.
///
/// # Errors
/// The supplied file path must have a valid extension: parquet, parq, or pq.
pub async fn get_file_iterator(
    store_factory: &ObjectStoreFactory,
    file_path: &Url,
    batch_size: Option<usize>,
) -> Result<BlockingStream<ParquetRecordBatchStream<ParquetObjectReader>>, ArrowError> {
    get_file_iterator_for_row_group_range(store_factory, file_path, None, None, batch_size).await
}

/// Creates an iterator of Arrow record batches from a Parquet file.
///
/// This allows for the row groups to be read from the Parquet file
/// to be specified.
///
/// # Examples
/// ```ignore
/// # use url::Url;
///
/// let s = get_file_iterator_for_row_group_range(store_factory, Url::parse("/path/to/file.parquet").unwrap(), Some(0), Some(10), None);
/// ```
///
/// # Errors
/// The supplied file path must have a valid extension: parquet, parq, or pq.
///
/// The end row group, if specified, must be greater than or equal to the start row group.
async fn get_file_iterator_for_row_group_range(
    store_factory: &ObjectStoreFactory,
    src: &Url,
    start_row_group: Option<usize>,
    end_row_group: Option<usize>,
    batch_size: Option<usize>,
) -> Result<BlockingStream<ParquetRecordBatchStream<ParquetObjectReader>>, ArrowError> {
    let extension = PathBuf::from(src.path())
        .extension()
        .map(|ext| ext.to_str().unwrap_or_default().to_owned());
    if let Some("parquet" | "parq" | "pq") = extension.as_deref() {
        let mut builder = get_parquet_builder(store_factory, src).await?;

        if let Some(num_groups) = batch_size {
            builder = builder.with_batch_size(num_groups);
        }

        let num_row_groups = builder.metadata().num_row_groups();
        let num_rows = builder.metadata().file_metadata().num_rows();

        let rg_start = start_row_group.unwrap_or(0);
        let rg_end = end_row_group.unwrap_or(num_row_groups);

        if rg_end < rg_start {
            return Err(ArrowError::InvalidArgumentError(
                "end row group must not be less than start".to_owned(),
            ));
        }

        info!(
            "Creating input iterator for file {} row groups {} rows {} loading row groups {}..{}",
            src.as_ref(),
            num_row_groups.to_formatted_string(&Locale::en),
            num_rows.to_formatted_string(&Locale::en),
            rg_start.to_formatted_string(&Locale::en),
            rg_end.to_formatted_string(&Locale::en)
        );

        builder = builder.with_row_groups((rg_start..rg_end).collect());

        builder
            .build()
            .map(futures::executor::block_on_stream)
            .map_err(ArrowError::from)
    } else {
        let p = extension.unwrap_or("<none>".to_owned());
        Err(ArrowError::IoError(format!("Unrecognised extension {p}")))
    }
}

/// Create an asynchronous builder for reading Parquet files from an object store.
///
/// The URL must start with a scheme that the object store recognises, e.g. "file" or "s3".
///
/// # Errors
/// This function will return an error if it couldn't connect to S3 or open a valid
/// Parquet file.
pub async fn get_parquet_builder(
    store_factory: &ObjectStoreFactory,
    src: &Url,
) -> Result<ParquetRecordBatchStreamBuilder<ParquetObjectReader>, ArrowError> {
    let store = store_factory.get_object_store(src)?;

    // HEAD the file to get metadata
    let path = object_store::path::Path::from(src.path());
    let object_meta = store
        .head(&path)
        .await
        .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

    // Create a reader for the target file, use it to construct a Stream
    let reader = ParquetObjectReader::new(store, object_meta);
    Ok(ParquetRecordBatchStreamBuilder::new(reader).await?)
}

/// Read the schema from the given Parquet file.
///
/// The Parquet file metadata is read and returned.
///
/// # Examples
/// ```no_run
/// # use compaction::read_schema;
/// # use std::sync::Arc;
/// # use arrow::datatypes::Schema;
/// # use aws_types::region::Region;
/// # use url::Url;
/// # use crate::compaction::ObjectStoreFactory;
/// let sf = ObjectStoreFactory::new(None, &Region::new("eu-west-2"));
/// let schema: Arc<Schema> = read_schema(&sf, &Url::parse("file:///path/to/my/file").unwrap()).unwrap();
/// ```
/// # Errors
/// Errors can occur if we are not able to read the named file or if the schema/file is corrupt.
///
pub fn read_schema(
    store_factory: &ObjectStoreFactory,
    src: &Url,
) -> Result<Arc<Schema>, ArrowError> {
    let builder = futures::executor::block_on(get_parquet_builder(store_factory, src))?;
    let stream = builder.build()?;

    Ok(stream.schema().clone())
}

/// Checks all files match the schema.
///
/// The schema should be provided from the first file, all subsequent
/// file schemas are compared to this. This function returns true if all match.
#[must_use]
pub fn validate_schemas_same(
    store_factory: &ObjectStoreFactory,
    input_file_paths: &[Url],
    schema: &Arc<Schema>,
) -> bool {
    input_file_paths
        .iter()
        .skip(1)
        .map(|u| read_schema(store_factory, u))
        .all(|s| s.as_ref().unwrap_or(&Arc::new(Schema::empty())) == schema)
}
