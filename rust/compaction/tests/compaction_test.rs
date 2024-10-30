use std::{collections::HashMap, fs::File, sync::Arc};

use arrow::{
    array::{Int32Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use color_eyre::eyre::Error;
use compaction::{merge_sorted_files, ColRange, CompactionInput, PartitionBound};
use datafusion::parquet::{
    arrow::{arrow_reader::ArrowReaderBuilder, ArrowWriter},
    basic::Compression,
    file::properties::WriterProperties,
};
use tempfile::{tempdir, TempDir};
use url::Url;

#[tokio::test]
async fn should_merge_two_files() -> Result<(), Error> {
    // Given
    let dir = tempdir()?;
    let file_1 = file(&dir, "file1.parquet");
    let file_2 = file(&dir, "file2.parquet");
    let output = file(&dir, "output.parquet");
    write_file_of_ints(&file_1, "key", Int32Array::from(vec![1, 3]))?;
    write_file_of_ints(&file_2, "key", Int32Array::from(vec![2, 4]))?;

    let input = CompactionInput {
        input_files: Vec::from([file_1, file_2]),
        output_file: output.clone(),
        row_key_cols: Vec::from(["key".to_string()]),
        region: single_int_range("key", 0, 5),
        ..Default::default()
    };

    // When
    let result = merge_sorted_files(&input).await?;

    // Then
    assert_eq!(read_file_of_ints(&output, "key")?, vec![1, 2, 3, 4]);
    assert_eq!([result.rows_read, result.rows_written], [4, 4]);
    Ok(())
}

#[tokio::test]
async fn should_merge_files_with_overlapping_data() -> Result<(), Error> {
    // Given
    let dir = tempdir()?;
    let file_1 = file(&dir, "file1.parquet");
    let file_2 = file(&dir, "file2.parquet");
    let output = file(&dir, "output.parquet");
    write_file_of_ints(&file_1, "key", Int32Array::from(vec![1, 2]))?;
    write_file_of_ints(&file_2, "key", Int32Array::from(vec![2, 3]))?;

    let input = CompactionInput {
        input_files: Vec::from([file_1, file_2]),
        output_file: output.clone(),
        row_key_cols: Vec::from(["key".to_string()]),
        region: single_int_range("key", 0, 5),
        ..Default::default()
    };

    // When
    let result = merge_sorted_files(&input).await?;

    // Then
    assert_eq!(read_file_of_ints(&output, "key")?, vec![1, 2, 2, 3]);
    assert_eq!([result.rows_read, result.rows_written], [4, 4]);
    Ok(())
}

#[tokio::test]
async fn should_exclude_data_not_in_region() -> Result<(), Error> {
    // Given
    let dir = tempdir()?;
    let file_1 = file(&dir, "file1.parquet");
    let file_2 = file(&dir, "file2.parquet");
    let output = file(&dir, "output.parquet");
    write_file_of_ints(&file_1, "key", Int32Array::from(vec![1, 2]))?;
    write_file_of_ints(&file_2, "key", Int32Array::from(vec![3, 4]))?;

    let input = CompactionInput {
        input_files: Vec::from([file_1, file_2]),
        output_file: output.clone(),
        row_key_cols: Vec::from(["key".to_string()]),
        region: single_int_range("key", 2, 4),
        ..Default::default()
    };

    // When
    let result = merge_sorted_files(&input).await?;

    // Then
    assert_eq!(read_file_of_ints(&output, "key")?, vec![2, 3]);
    assert_eq!([result.rows_read, result.rows_written], [2, 2]);
    Ok(())
}

fn file(dir: &TempDir, name: &str) -> Url {
    Url::from_file_path(dir.path().join(name)).unwrap()
}

fn write_file_of_ints(path: &Url, field_name: &str, data: Int32Array) -> Result<(), Error> {
    let schema = Schema::new(vec![Field::new(field_name, DataType::Int32, false)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(data)])?;
    write_file(path, batch)
}

fn write_file(path: &Url, batch: RecordBatch) -> Result<(), Error> {
    let file = File::create_new(path.path())?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), writer_props())?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn writer_props() -> Option<WriterProperties> {
    Some(
        WriterProperties::builder()
            .set_writer_version(datafusion::parquet::file::properties::WriterVersion::PARQUET_2_0)
            .set_compression(Compression::ZSTD(Default::default()))
            .set_column_index_truncate_length(Some(128))
            .set_statistics_truncate_length(Some(2147483647))
            .set_dictionary_enabled(true)
            .build(),
    )
}

fn read_file_of_ints(path: &Url, field_name: &str) -> Result<Vec<i32>, Error> {
    let file = File::open(path.path())?;
    let mut data: Vec<i32> = Vec::new();
    for result in ArrowReaderBuilder::try_new(file)?.build()? {
        let batch = result?;
        let batch_data = batch
            .column_by_name(field_name)
            .ok_or_else(|| Error::msg("field not found"))?
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| Error::msg("could not read field as an integer"))?;
        data.extend(batch_data.values());
    }
    Ok(data)
}

fn single_int_range(field_name: &str, min: i32, max: i32) -> HashMap<String, ColRange<'_>> {
    return HashMap::from([(field_name.to_string(), int_range(min, max))]);
}

fn int_range<'r>(min: i32, max: i32) -> ColRange<'r> {
    ColRange {
        lower: PartitionBound::Int32(min),
        lower_inclusive: true,
        upper: PartitionBound::Int32(max),
        upper_inclusive: false,
    }
}
