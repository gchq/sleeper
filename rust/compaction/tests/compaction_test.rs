mod compaction_helpers;

use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema};
use color_eyre::eyre::Error;
use compaction::{merge_sorted_files, CompactionInput};
use compaction_helpers::*;
use tempfile::tempdir;

#[tokio::test]
async fn should_merge_two_files() -> Result<(), Error> {
    // Given
    let dir = tempdir()?;
    let file_1 = file(&dir, "file1.parquet");
    let file_2 = file(&dir, "file2.parquet");
    let output = file(&dir, "output.parquet");
    let sketches = file(&dir, "output.sketches");
    write_file_of_ints(&file_1, "key", vec![1, 3])?;
    write_file_of_ints(&file_2, "key", vec![2, 4])?;

    let input = CompactionInput {
        input_files: Vec::from([file_1, file_2]),
        output_file: output.clone(),
        row_key_cols: row_key_cols(["key"]),
        region: single_int_range("key", 0, 5),
        ..Default::default()
    };

    // When
    let result = merge_sorted_files(&input).await?;

    // Then
    assert_eq!(read_file_of_ints(&output, "key")?, vec![1, 2, 3, 4]);
    assert_eq!([result.rows_read, result.rows_written], [4, 4]);
    assert_eq!(read_sketch_min_max_ints(&sketches)?, [1, 4]);
    Ok(())
}

#[tokio::test]
async fn should_merge_files_with_overlapping_data() -> Result<(), Error> {
    // Given
    let dir = tempdir()?;
    let file_1 = file(&dir, "file1.parquet");
    let file_2 = file(&dir, "file2.parquet");
    let output = file(&dir, "output.parquet");
    let sketches = file(&dir, "output.sketches");
    write_file_of_ints(&file_1, "key", vec![1, 2])?;
    write_file_of_ints(&file_2, "key", vec![2, 3])?;

    let input = CompactionInput {
        input_files: Vec::from([file_1, file_2]),
        output_file: output.clone(),
        row_key_cols: row_key_cols(["key"]),
        region: single_int_range("key", 0, 5),
        ..Default::default()
    };

    // When
    let result = merge_sorted_files(&input).await?;

    // Then
    assert_eq!(read_file_of_ints(&output, "key")?, vec![1, 2, 2, 3]);
    assert_eq!([result.rows_read, result.rows_written], [4, 4]);
    assert_eq!(read_sketch_min_max_ints(&sketches)?, [1, 3]);
    Ok(())
}

#[tokio::test]
async fn should_exclude_data_not_in_region() -> Result<(), Error> {
    // Given
    let dir = tempdir()?;
    let file_1 = file(&dir, "file1.parquet");
    let file_2 = file(&dir, "file2.parquet");
    let output = file(&dir, "output.parquet");
    let sketches = file(&dir, "output.sketches");
    write_file_of_ints(&file_1, "key", vec![1, 2])?;
    write_file_of_ints(&file_2, "key", vec![3, 4])?;

    let input = CompactionInput {
        input_files: Vec::from([file_1, file_2]),
        output_file: output.clone(),
        row_key_cols: row_key_cols(["key"]),
        region: single_int_range("key", 2, 4),
        ..Default::default()
    };

    // When
    let result = merge_sorted_files(&input).await?;

    // Then
    assert_eq!(read_file_of_ints(&output, "key")?, vec![2, 3]);
    assert_eq!([result.rows_read, result.rows_written], [2, 2]);
    assert_eq!(read_sketch_min_max_ints(&sketches)?, [2, 3]);
    Ok(())
}

#[tokio::test]
async fn should_exclude_data_not_in_multidimensional_region() -> Result<(), Error> {
    // Given
    let dir = tempdir()?;
    let file_1 = file(&dir, "file1.parquet");
    let file_2 = file(&dir, "file2.parquet");
    let output = file(&dir, "output.parquet");
    let sketches = file(&dir, "output.sketches");
    let schema = Arc::new(Schema::new(vec![
        Field::new("key1", DataType::Int32, false),
        Field::new("key2", DataType::Int32, false),
    ]));
    let data_1 = batch_of_int_fields(schema.clone(), [vec![1, 2, 3], vec![11, 12, 13]])?;
    let data_2 = batch_of_int_fields(schema.clone(), [vec![2, 3, 4], vec![22, 23, 24]])?;
    write_file(&file_1, &data_1)?;
    write_file(&file_2, &data_2)?;

    let input = CompactionInput {
        input_files: Vec::from([file_1, file_2]),
        output_file: output.clone(),
        row_key_cols: row_key_cols(["key1", "key2"]),
        region: HashMap::from([
            region_entry("key1", int_range(2, 4)),
            region_entry("key2", int_range(13, 23)),
        ]),
        ..Default::default()
    };

    // When
    let result = merge_sorted_files(&input).await?;

    // Then
    assert_eq!(
        read_file_of_int_fields(&output, ["key1", "key2"])?,
        vec![[2, 22], [3, 13]]
    );
    assert_eq!([result.rows_read, result.rows_written], [2, 2]);
    assert_eq!(read_sketch_min_max_ints(&sketches)?, [2, 3]);
    Ok(())
}
