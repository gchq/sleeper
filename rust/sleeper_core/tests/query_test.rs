/*
 * Copyright 2022-2026 Crown Copyright
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
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use sleeper_core::{
    CommonConfigBuilder, CompletedOutput, LeafPartitionQueryConfig, OutputType,
    SleeperParquetOptions, SleeperRegion, filter_aggregation_config::aggregate::Aggregate,
    run_query, sleeper_context::SleeperContext,
};
use std::collections::HashMap;
use std::sync::Arc;
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
        sql_query: None,
    };

    // When
    let result = run_query(&query_config, &SleeperContext::default()).await?;

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
        sql_query: None,
    };

    // When
    let result = run_query(&query_config, &SleeperContext::default()).await?;

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
        sql_query: None,
    };

    // When
    let result = run_query(&query_config, &SleeperContext::default()).await?;

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
        sql_query: None,
    };

    // When
    let result = run_query(&query_config, &SleeperContext::default()).await?;

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
        sql_query: None,
    };

    // When
    let result = run_query(&query_config, &SleeperContext::default()).await?;

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
        sql_query: None,
    };

    // When
    let result = run_query(&query_config, &SleeperContext::default()).await?;

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
        sql_query: None,
    };

    // Then
    let Err(result) = run_query(&query_config, &SleeperContext::default()).await else {
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
        sql_query: None,
    };

    // When
    let result = run_query(&query_config, &SleeperContext::default()).await?;

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
        sql_query: None,
    };

    // When
    let result = run_query(&query_config, &SleeperContext::default()).await?;

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

#[tokio::test]
async fn should_filter_results_with_sql_query_where_clause() -> Result<(), Error> {
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
        .region(SleeperRegion::new(single_int_range("key", 0, 11)))
        .output(OutputType::ArrowRecordBatch)
        .build()?;

    let query_config = LeafPartitionQueryConfig {
        common: input,
        explain_plans: false,
        ranges: vec![SleeperRegion::new(single_int_range("key", 0, 11))],
        requested_value_fields: None,
        sql_query: Some("SELECT * FROM query_results WHERE key > 4 AND key < 9;".to_string()),
    };

    // When
    let result = run_query(&query_config, &SleeperContext::default()).await?;

    // Then
    let CompletedOutput::ArrowRecordBatch(stream) = result else {
        bail!("Expected arrow record batch stream output");
    };

    assert_eq!(
        read_batches_of_int_fields(stream, ["key"]).await?,
        vec![[5], [6], [7], [8]]
    );
    Ok(())
}

#[tokio::test]
async fn should_apply_sql_query_with_range_and_sql_filter() -> Result<(), Error> {
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
        .region(SleeperRegion::new(single_int_range("key", 0, 11)))
        .output(OutputType::ArrowRecordBatch)
        .build()?;

    let query_config = LeafPartitionQueryConfig {
        common: input,
        explain_plans: false,
        ranges: vec![SleeperRegion::new(single_int_range("key", 2, 9))],
        requested_value_fields: None,
        sql_query: Some("SELECT * FROM query_results WHERE key > 3 AND key < 8;".to_string()),
    };

    // When
    let result = run_query(&query_config, &SleeperContext::default()).await?;

    // Then
    let CompletedOutput::ArrowRecordBatch(stream) = result else {
        bail!("Expected arrow record batch stream output");
    };

    assert_eq!(
        read_batches_of_int_fields(stream, ["key"]).await?,
        vec![[4], [5], [6], [7]]
    );
    Ok(())
}

#[tokio::test]
async fn should_apply_sql_query_with_empty_result_set() -> Result<(), Error> {
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
        .region(SleeperRegion::new(single_int_range("key", 0, 11)))
        .output(OutputType::ArrowRecordBatch)
        .build()?;

    let query_config = LeafPartitionQueryConfig {
        common: input,
        explain_plans: false,
        ranges: vec![SleeperRegion::new(single_int_range("key", 0, 11))],
        requested_value_fields: None,
        sql_query: Some("SELECT * FROM query_results WHERE key > 100;".to_string()),
    };

    // When
    let result = run_query(&query_config, &SleeperContext::default()).await?;

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
async fn should_apply_sql_query_with_limit() -> Result<(), Error> {
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
        .region(SleeperRegion::new(single_int_range("key", 0, 11)))
        .output(OutputType::ArrowRecordBatch)
        .build()?;

    let query_config = LeafPartitionQueryConfig {
        common: input,
        explain_plans: false,
        ranges: vec![SleeperRegion::new(single_int_range("key", 0, 11))],
        requested_value_fields: None,
        sql_query: Some("SELECT * FROM query_results LIMIT 5;".to_string()),
    };

    // When
    let result = run_query(&query_config, &SleeperContext::default()).await?;

    // Then
    let CompletedOutput::ArrowRecordBatch(stream) = result else {
        bail!("Expected arrow record batch stream output");
    };

    let results = read_batches_of_int_fields(stream, ["key"]).await?;
    assert_eq!(results.len(), 5);
    assert_eq!(results, vec![[1], [2], [3], [4], [5]]);
    Ok(())
}

#[tokio::test]
async fn should_apply_sql_query_with_reverse_sort() -> Result<(), Error> {
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
        .region(SleeperRegion::new(single_int_range("key", 0, 11)))
        .output(OutputType::ArrowRecordBatch)
        .build()?;

    let query_config = LeafPartitionQueryConfig {
        common: input,
        explain_plans: false,
        ranges: vec![SleeperRegion::new(single_int_range("key", 0, 11))],
        requested_value_fields: None,
        sql_query: Some("SELECT * FROM query_results ORDER BY key DESC;".to_string()),
    };

    // When
    let result = run_query(&query_config, &SleeperContext::default()).await?;

    // Then
    let CompletedOutput::ArrowRecordBatch(stream) = result else {
        bail!("Expected arrow record batch stream output");
    };

    assert_eq!(
        read_batches_of_int_fields(stream, ["key"]).await?,
        vec![[10], [9], [8], [7], [6], [5], [4], [3], [2], [1]]
    );
    Ok(())
}

#[tokio::test]
async fn should_apply_sql_query_with_limit_and_reverse_sort() -> Result<(), Error> {
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
        .region(SleeperRegion::new(single_int_range("key", 0, 11)))
        .output(OutputType::ArrowRecordBatch)
        .build()?;

    let query_config = LeafPartitionQueryConfig {
        common: input,
        explain_plans: false,
        ranges: vec![SleeperRegion::new(single_int_range("key", 0, 11))],
        requested_value_fields: None,
        sql_query: Some("SELECT * FROM query_results ORDER BY key DESC LIMIT 3;".to_string()),
    };

    // When
    let result = run_query(&query_config, &SleeperContext::default()).await?;

    // Then
    let CompletedOutput::ArrowRecordBatch(stream) = result else {
        bail!("Expected arrow record batch stream output");
    };

    assert_eq!(
        read_batches_of_int_fields(stream, ["key"]).await?,
        vec![[10], [9], [8]]
    );
    Ok(())
}

#[tokio::test]
async fn should_apply_sql_query_after_sleeper_aggregate() -> Result<(), Error> {
    // Given
    let dir = tempdir()?;
    let file_1 = file(&dir, "file1.parquet");
    let file_2 = file(&dir, "file2.parquet");

    let schema = Arc::new(Schema::new(vec![
        Field::new("row_key", DataType::Int32, false),
        Field::new("sort_key", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let data_1 = batch_of_int_fields(schema.clone(), [vec![1, 1], vec![10, 11], vec![100, 200]])?;
    let data_2 = batch_of_int_fields(schema.clone(), [vec![1, 1], vec![12, 13], vec![300, 400]])?;

    write_file(&file_1, &data_1)?;
    write_file(&file_2, &data_2)?;

    let aggregates = Aggregate::parse_config("sum(value)")?;

    let input = CommonConfigBuilder::new()
        .input_files(vec![file_1, file_2])
        .input_files_sorted(true)
        .row_key_cols(col_names(["row_key"]))
        .sort_key_cols(col_names(["sort_key"]))
        .region(SleeperRegion::new(HashMap::from([region_entry(
            "row_key",
            int_range(0, 100),
        )])))
        .aggregates(aggregates)
        .output(OutputType::ArrowRecordBatch)
        .build()?;

    let query_config = LeafPartitionQueryConfig {
        common: input,
        explain_plans: false,
        ranges: vec![SleeperRegion::new(HashMap::from([region_entry(
            "row_key",
            int_range(0, 100),
        )]))],
        requested_value_fields: None,
        sql_query: Some("SELECT * FROM query_results WHERE sort_key > 11;".to_string()),
    };

    // When
    let result = run_query(&query_config, &SleeperContext::default()).await?;

    // Then
    let CompletedOutput::ArrowRecordBatch(stream) = result else {
        bail!("Expected arrow record batch stream output");
    };

    let results = read_batches_of_int_fields(stream, ["row_key", "sort_key", "value"]).await?;
    assert_eq!(results, vec![[1, 12, 300], [1, 13, 400]]);
    Ok(())
}

#[tokio::test]
async fn should_apply_sql_query_count_with_sort_column() -> Result<(), Error> {
    // Given
    let dir = tempdir()?;
    let file_1 = file(&dir, "file1.parquet");
    let file_2 = file(&dir, "file2.parquet");

    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("timestamp", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let data_1 = batch_of_int_fields(
        schema.clone(),
        [vec![1, 3, 5], vec![100, 110, 120], vec![10, 30, 50]],
    )?;
    let data_2 = batch_of_int_fields(schema.clone(), [vec![2, 4], vec![105, 115], vec![20, 40]])?;

    write_file(&file_1, &data_1)?;
    write_file(&file_2, &data_2)?;

    let input = CommonConfigBuilder::new()
        .input_files(vec![file_1, file_2])
        .input_files_sorted(true)
        .row_key_cols(col_names(["key"]))
        .sort_key_cols(col_names(["timestamp"]))
        .region(SleeperRegion::new(HashMap::from([region_entry(
            "key",
            int_range(0, 10),
        )])))
        .output(OutputType::ArrowRecordBatch)
        .build()?;

    let query_config = LeafPartitionQueryConfig {
        common: input,
        explain_plans: false,
        ranges: vec![SleeperRegion::new(HashMap::from([region_entry(
            "key",
            int_range(0, 10),
        )]))],
        requested_value_fields: None,
        sql_query: Some("SELECT CAST(count(*) AS INT) as count FROM query_results;".to_string()),
    };

    // When
    let result = run_query(&query_config, &SleeperContext::default()).await?;

    // Then
    let CompletedOutput::ArrowRecordBatch(stream) = result else {
        bail!("Expected arrow record batch stream output");
    };

    let results = read_batches_of_int_fields(stream, ["count"]).await?;
    assert_eq!(results, vec![[5]]);
    Ok(())
}

#[tokio::test]
async fn should_apply_sql_query_select_key_with_sort_column() -> Result<(), Error> {
    // Given
    let dir = tempdir()?;
    let file_1 = file(&dir, "file1.parquet");
    let file_2 = file(&dir, "file2.parquet");

    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("timestamp", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let data_1 = batch_of_int_fields(
        schema.clone(),
        [vec![1, 3, 5], vec![100, 110, 120], vec![10, 30, 50]],
    )?;
    let data_2 = batch_of_int_fields(schema.clone(), [vec![2, 4], vec![105, 115], vec![20, 40]])?;

    write_file(&file_1, &data_1)?;
    write_file(&file_2, &data_2)?;

    let input = CommonConfigBuilder::new()
        .input_files(vec![file_1, file_2])
        .input_files_sorted(true)
        .row_key_cols(col_names(["key"]))
        .sort_key_cols(col_names(["timestamp"]))
        .region(SleeperRegion::new(HashMap::from([region_entry(
            "key",
            int_range(0, 10),
        )])))
        .output(OutputType::ArrowRecordBatch)
        .build()?;

    let query_config = LeafPartitionQueryConfig {
        common: input,
        explain_plans: false,
        ranges: vec![SleeperRegion::new(HashMap::from([region_entry(
            "key",
            int_range(0, 10),
        )]))],
        requested_value_fields: None,
        sql_query: Some("SELECT key FROM query_results;".to_string()),
    };

    // When
    let result = run_query(&query_config, &SleeperContext::default()).await?;

    // Then
    let CompletedOutput::ArrowRecordBatch(stream) = result else {
        bail!("Expected arrow record batch stream output");
    };

    assert_eq!(
        read_batches_of_int_fields(stream, ["key"]).await?,
        vec![[1], [2], [3], [4], [5]]
    );
    Ok(())
}
