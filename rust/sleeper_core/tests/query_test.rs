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

use color_eyre::eyre::{Error, bail};
use sleeper_core::{
    CommonConfigBuilder, CompletedOutput, LeafPartitionQueryConfig, OutputType,
    SleeperParquetOptions, SleeperRegion, run_query,
};
use tempfile::tempdir;
use test_util::*;

#[tokio::test]
async fn should_return_subset_results_with_query_subset_of_partition() -> Result<(), Error> {
    // Given
    let dir = tempdir()?;
    let file_1 = file(&dir, "file1.parquet");
    let file_2 = file(&dir, "file2.parquet");

    write_file_of_ints(&file_1, "key", vec![1, 3])?;
    write_file_of_ints(&file_2, "key", vec![2, 4])?;

    let input = CommonConfigBuilder::new()
        .input_files(Vec::from([file_1, file_2]))
        .input_files_sorted(true)
        .row_key_cols(col_names(["key"]))
        .region(SleeperRegion::new(single_int_range("key", 0, 5)))
        .output(OutputType::ArrowRecordBatch)
        .build()?;

    let query_config = LeafPartitionQueryConfig {
        common: input,
        explain_plans: false,
        ranges: vec![SleeperRegion::new(single_int_range("key", 2, 4))],
        requested_value_fields: None,
    };

    // When
    let result = run_query(&query_config).await?;

    // Then
    let CompletedOutput::ArrowRecordBatch(stream) = result else {
        bail!("Expected arrow record batch stream output");
    };

    assert_eq!(
        read_batches_of_int_fields(stream, ["key"]).await?,
        vec![[2], [3]]
    );
    Ok(())
}

#[tokio::test]
async fn should_return_subset_results_with_query_subset_of_partition_unsorted_input()
-> Result<(), Error> {
    // Given
    let dir = tempdir()?;
    let file_1 = file(&dir, "file1.parquet");
    let file_2 = file(&dir, "file2.parquet");

    write_file_of_ints(&file_1, "key", vec![7, 3, 5, 1])?;
    write_file_of_ints(&file_2, "key", vec![8, 6, 2, 4])?;

    let input = CommonConfigBuilder::new()
        .input_files(vec![file_1, file_2])
        .input_files_sorted(false)
        .row_key_cols(col_names(["key"]))
        .region(SleeperRegion::new(single_int_range("key", 1, 7)))
        .output(OutputType::ArrowRecordBatch)
        .build()?;

    let query_config = LeafPartitionQueryConfig {
        common: input,
        explain_plans: false,
        ranges: vec![SleeperRegion::new(single_int_range("key", 2, 6))],
        requested_value_fields: None,
    };

    // When
    let result = run_query(&query_config).await?;

    // Then
    let CompletedOutput::ArrowRecordBatch(stream) = result else {
        bail!("Expected arrow record batch stream output");
    };

    assert_eq!(
        read_batches_of_int_fields(stream, ["key"]).await?,
        vec![[2], [3], [4], [5]]
    );
    Ok(())
}

#[tokio::test]
async fn should_return_subset_results_with_overlapping_query_and_partition_range()
-> Result<(), Error> {
    // Given
    let dir = tempdir()?;
    let file_1 = file(&dir, "file1.parquet");
    let file_2 = file(&dir, "file2.parquet");

    write_file_of_ints(&file_1, "key", vec![1, 3, 5, 7, 9])?;
    write_file_of_ints(&file_2, "key", vec![2, 4, 6, 8, 10])?;

    let input = CommonConfigBuilder::new()
        .input_files(vec![file_1, file_2])
        .input_files_sorted(true)
        .row_key_cols(col_names(["key"]))
        .region(SleeperRegion::new(single_int_range("key", 0, 6)))
        .output(OutputType::ArrowRecordBatch)
        .build()?;

    let query_config = LeafPartitionQueryConfig {
        common: input,
        explain_plans: false,
        ranges: vec![SleeperRegion::new(single_int_range("key", 2, 9))],
        requested_value_fields: None,
    };

    // When
    let result = run_query(&query_config).await?;

    // Then
    let CompletedOutput::ArrowRecordBatch(stream) = result else {
        bail!("Expected arrow record batch stream output");
    };

    assert_eq!(
        read_batches_of_int_fields(stream, ["key"]).await?,
        vec![[2], [3], [4], [5]]
    );
    Ok(())
}

#[tokio::test]
async fn should_return_zero_results_with_non_overlapping_query_and_partition_range()
-> Result<(), Error> {
    // Given
    let dir = tempdir()?;
    let file_1 = file(&dir, "file1.parquet");
    let file_2 = file(&dir, "file2.parquet");

    write_file_of_ints(&file_1, "key", vec![1, 3, 5, 7, 9])?;
    write_file_of_ints(&file_2, "key", vec![2, 4, 6, 8, 10])?;

    let input = CommonConfigBuilder::new()
        .input_files(vec![file_1, file_2])
        .input_files_sorted(true)
        .row_key_cols(col_names(["key"]))
        .region(SleeperRegion::new(single_int_range("key", 0, 3)))
        .output(OutputType::ArrowRecordBatch)
        .build()?;

    let query_config = LeafPartitionQueryConfig {
        common: input,
        explain_plans: false,
        ranges: vec![SleeperRegion::new(single_int_range("key", 6, 9))],
        requested_value_fields: None,
    };

    // When
    let result = run_query(&query_config).await?;

    // Then
    let CompletedOutput::ArrowRecordBatch(stream) = result else {
        bail!("Expected arrow record batch stream output");
    };

    assert_eq!(
        read_batches_of_int_fields(stream, ["key"]).await?,
        Vec::<[i32; 1]>::new()
    );
    Ok(())
}

#[tokio::test]
async fn should_return_results_from_two_overlapping_query_ranges() -> Result<(), Error> {
    // Given
    let dir = tempdir()?;
    let file_1 = file(&dir, "file1.parquet");
    let file_2 = file(&dir, "file2.parquet");

    write_file_of_ints(&file_1, "key", vec![1, 3, 5, 7, 9])?;
    write_file_of_ints(&file_2, "key", vec![2, 4, 6, 8, 10])?;

    let input = CommonConfigBuilder::new()
        .input_files(vec![file_1, file_2])
        .input_files_sorted(true)
        .row_key_cols(col_names(["key"]))
        .region(SleeperRegion::new(single_int_range("key", -10, 11)))
        .output(OutputType::ArrowRecordBatch)
        .build()?;

    let query_config = LeafPartitionQueryConfig {
        common: input,
        explain_plans: false,
        ranges: vec![
            SleeperRegion::new(single_int_range("key", 2, 6)),
            SleeperRegion::new(single_int_range("key", 4, 9)),
        ],
        requested_value_fields: None,
    };

    // When
    let result = run_query(&query_config).await?;

    // Then
    let CompletedOutput::ArrowRecordBatch(stream) = result else {
        bail!("Expected arrow record batch stream output");
    };

    assert_eq!(
        read_batches_of_int_fields(stream, ["key"]).await?,
        vec![[2], [3], [4], [5], [6], [7], [8]]
    );
    Ok(())
}

#[tokio::test]
async fn should_return_results_from_two_non_overlapping_query_ranges() -> Result<(), Error> {
    // Given
    let dir = tempdir()?;
    let file_1 = file(&dir, "file1.parquet");
    let file_2 = file(&dir, "file2.parquet");

    write_file_of_ints(&file_1, "key", vec![1, 3, 5, 7, 9])?;
    write_file_of_ints(&file_2, "key", vec![2, 4, 6, 8, 10])?;

    let input = CommonConfigBuilder::new()
        .input_files(vec![file_1, file_2])
        .input_files_sorted(true)
        .row_key_cols(col_names(["key"]))
        .region(SleeperRegion::new(single_int_range("key", -10, 11)))
        .output(OutputType::ArrowRecordBatch)
        .build()?;

    let query_config = LeafPartitionQueryConfig {
        common: input,
        explain_plans: false,
        ranges: vec![
            SleeperRegion::new(single_int_range("key", 2, 5)),
            SleeperRegion::new(single_int_range("key", 7, 9)),
        ],
        requested_value_fields: None,
    };

    // When
    let result = run_query(&query_config).await?;

    // Then
    let CompletedOutput::ArrowRecordBatch(stream) = result else {
        bail!("Expected arrow record batch stream output");
    };

    assert_eq!(
        read_batches_of_int_fields(stream, ["key"]).await?,
        vec![[2], [3], [4], [7], [8]]
    );
    Ok(())
}

#[tokio::test]
async fn should_error_with_no_query_ranges() -> Result<(), Error> {
    // Given
    let dir = tempdir()?;
    let file_1 = file(&dir, "file1.parquet");
    let file_2 = file(&dir, "file2.parquet");

    write_file_of_ints(&file_1, "key", vec![1, 3, 5, 7, 9])?;
    write_file_of_ints(&file_2, "key", vec![2, 4, 6, 8, 10])?;

    let input = CommonConfigBuilder::new()
        .input_files(vec![file_1, file_2])
        .input_files_sorted(true)
        .row_key_cols(col_names(["key"]))
        .region(SleeperRegion::new(single_int_range("key", 0, 3)))
        .output(OutputType::ArrowRecordBatch)
        .build()?;

    let query_config = LeafPartitionQueryConfig {
        common: input,
        explain_plans: false,
        ranges: vec![],
        requested_value_fields: None,
    };

    // Then
    let Err(result) = run_query(&query_config).await else {
        bail!("Expected an error type here");
    };

    assert_eq!(
        format!("{result}"),
        "Error during planning: No query regions specified"
    );
    Ok(())
}

#[tokio::test]
async fn should_return_results_as_file_with_sketch() -> Result<(), Error> {
    // Given
    let dir = tempdir()?;
    let file_1 = file(&dir, "file1.parquet");
    let file_2 = file(&dir, "file2.parquet");
    let output = file(&dir, "output.parquet");
    let sketches = file(&dir, "output.sketches");

    write_file_of_ints(&file_1, "key", vec![1, 3, 5, 7, 9])?;
    write_file_of_ints(&file_2, "key", vec![2, 4, 6, 8, 10])?;

    let input = CommonConfigBuilder::new()
        .input_files(vec![file_1, file_2])
        .input_files_sorted(true)
        .row_key_cols(col_names(["key"]))
        .region(SleeperRegion::new(single_int_range("key", 0, 6)))
        .output(OutputType::File {
            output_file: output.clone(),
            write_sketch_file: true,
            opts: SleeperParquetOptions::default(),
        })
        .build()?;

    let query_config = LeafPartitionQueryConfig {
        common: input,
        explain_plans: false,
        ranges: vec![SleeperRegion::new(single_int_range("key", 1, 5))],
        requested_value_fields: None,
    };

    // When
    let result = run_query(&query_config).await?;

    // Then
    let CompletedOutput::File(row_counts) = result else {
        bail!("Expected file output");
    };

    // Then
    assert_eq!(read_file_of_ints(&output, "key")?, vec![1, 2, 3, 4]);
    assert_eq!([row_counts.rows_read, row_counts.rows_written], [4, 4]);
    assert_eq!(read_sketch_min_max_ints(&sketches).await?, [1, 4]);
    Ok(())
}

#[tokio::test]
async fn should_return_results_as_file_without_sketch() -> Result<(), Error> {
    // Given
    let dir = tempdir()?;
    let file_1 = file(&dir, "file1.parquet");
    let file_2 = file(&dir, "file2.parquet");
    let output = file(&dir, "output.parquet");
    let sketches = file(&dir, "output.sketches");

    write_file_of_ints(&file_1, "key", vec![1, 3, 5, 7, 9])?;
    write_file_of_ints(&file_2, "key", vec![2, 4, 6, 8, 10])?;

    let input = CommonConfigBuilder::new()
        .input_files(vec![file_1, file_2])
        .input_files_sorted(true)
        .row_key_cols(col_names(["key"]))
        .region(SleeperRegion::new(single_int_range("key", 0, 6)))
        .output(OutputType::File {
            output_file: output.clone(),
            write_sketch_file: false,
            opts: SleeperParquetOptions::default(),
        })
        .build()?;

    let query_config = LeafPartitionQueryConfig {
        common: input,
        explain_plans: false,
        ranges: vec![SleeperRegion::new(single_int_range("key", 1, 5))],
        requested_value_fields: None,
    };

    // When
    let result = run_query(&query_config).await?;

    // Then
    let CompletedOutput::File(row_counts) = result else {
        bail!("Expected file output");
    };

    // Then
    assert_eq!(read_file_of_ints(&output, "key")?, vec![1, 2, 3, 4]);
    assert_eq!([row_counts.rows_read, row_counts.rows_written], [4, 4]);
    assert!(!sketches.to_file_path().unwrap().exists());
    Ok(())
}
