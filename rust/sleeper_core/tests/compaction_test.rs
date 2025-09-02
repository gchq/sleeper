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
mod compaction_helpers;

use arrow::datatypes::{DataType, Field, Schema};
use color_eyre::eyre::Error;
use compaction_helpers::*;
use sleeper_core::{
    CommonConfig, CompletionOptions, SleeperParquetOptions, SleeperPartitionRegion, run_compaction,
};
use std::{collections::HashMap, path::Path, sync::Arc};
use tempfile::tempdir;
use test_log::test;

#[test(tokio::test)]
async fn should_merge_two_files() -> Result<(), Error> {
    // Given
    let dir = tempdir()?;
    let file_1 = file(&dir, "file1.parquet");
    let file_2 = file(&dir, "file2.parquet");
    let output = file(&dir, "output.parquet");
    let sketches = file(&dir, "output.sketches");
    write_file_of_ints(&file_1, "key", vec![1, 3])?;
    write_file_of_ints(&file_2, "key", vec![2, 4])?;

    let input = CommonConfig {
        input_files: Vec::from([file_1, file_2]),
        input_files_sorted: true,
        row_key_cols: row_key_cols(["key"]),
        region: SleeperPartitionRegion::new(single_int_range("key", 0, 5)),
        output: CompletionOptions::File {
            output_file: output.clone(),
            opts: SleeperParquetOptions::default(),
        },
        ..Default::default()
    };

    // When
    let result = run_compaction(&input).await?;

    // Then
    assert_eq!(read_file_of_ints(&output, "key")?, vec![1, 2, 3, 4]);
    assert_eq!([result.rows_read, result.rows_written], [4, 4]);
    assert_eq!(read_sketch_min_max_ints(&sketches).await?, [1, 4]);
    Ok(())
}

#[test(tokio::test)]
async fn should_merge_files_with_overlapping_data() -> Result<(), Error> {
    // Given
    let dir = tempdir()?;
    let file_1 = file(&dir, "file1.parquet");
    let file_2 = file(&dir, "file2.parquet");
    let output = file(&dir, "output.parquet");
    let sketches = file(&dir, "output.sketches");
    write_file_of_ints(&file_1, "key", vec![1, 2])?;
    write_file_of_ints(&file_2, "key", vec![2, 3])?;

    let input = CommonConfig {
        input_files: Vec::from([file_1, file_2]),
        input_files_sorted: true,
        row_key_cols: row_key_cols(["key"]),
        region: SleeperPartitionRegion::new(single_int_range("key", 0, 5)),
        output: CompletionOptions::File {
            output_file: output.clone(),
            opts: SleeperParquetOptions::default(),
        },
        ..Default::default()
    };

    // When
    let result = run_compaction(&input).await?;

    // Then
    assert_eq!(read_file_of_ints(&output, "key")?, vec![1, 2, 2, 3]);
    assert_eq!([result.rows_read, result.rows_written], [4, 4]);
    assert_eq!(read_sketch_min_max_ints(&sketches).await?, [1, 3]);
    Ok(())
}

#[test(tokio::test)]
async fn should_exclude_data_not_in_region() -> Result<(), Error> {
    // Given
    let dir = tempdir()?;
    let file_1 = file(&dir, "file1.parquet");
    let file_2 = file(&dir, "file2.parquet");
    let output = file(&dir, "output.parquet");
    let sketches = file(&dir, "output.sketches");
    write_file_of_ints(&file_1, "key", vec![1, 2])?;
    write_file_of_ints(&file_2, "key", vec![3, 4])?;

    let input = CommonConfig {
        input_files: Vec::from([file_1, file_2]),
        input_files_sorted: true,
        row_key_cols: row_key_cols(["key"]),
        region: SleeperPartitionRegion::new(single_int_range("key", 2, 4)),
        output: CompletionOptions::File {
            output_file: output.clone(),
            opts: SleeperParquetOptions::default(),
        },
        ..Default::default()
    };

    // When
    let result = run_compaction(&input).await?;

    // Then
    assert_eq!(read_file_of_ints(&output, "key")?, vec![2, 3]);
    assert_eq!([result.rows_read, result.rows_written], [2, 2]);
    assert_eq!(read_sketch_min_max_ints(&sketches).await?, [2, 3]);
    Ok(())
}

#[test(tokio::test)]
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

    let input = CommonConfig {
        input_files: Vec::from([file_1, file_2]),
        input_files_sorted: true,
        row_key_cols: row_key_cols(["key1", "key2"]),
        region: SleeperPartitionRegion::new(HashMap::from([
            region_entry("key1", int_range(2, 4)),
            region_entry("key2", int_range(13, 23)),
        ])),
        output: CompletionOptions::File {
            output_file: output.clone(),
            opts: SleeperParquetOptions::default(),
        },
        ..Default::default()
    };

    // When
    let result = run_compaction(&input).await?;

    // Then
    assert_eq!(
        read_file_of_int_fields(&output, ["key1", "key2"])?,
        vec![[2, 22], [3, 13]]
    );
    assert_eq!([result.rows_read, result.rows_written], [2, 2]);
    assert_eq!(read_sketch_min_max_ints(&sketches).await?, [2, 3]);
    Ok(())
}

#[test(tokio::test)]
async fn should_compact_with_second_column_row_key() -> Result<(), Error> {
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
    let data_1 = batch_of_int_fields(schema.clone(), [vec![9, 8, 7], vec![11, 12, 13]])?;
    let data_2 = batch_of_int_fields(schema.clone(), [vec![54, 23, 44], vec![22, 23, 24]])?;
    write_file(&file_1, &data_1)?;
    write_file(&file_2, &data_2)?;

    let input = CommonConfig {
        input_files: Vec::from([file_1, file_2]),
        input_files_sorted: true,
        row_key_cols: row_key_cols(["key2"]),
        region: SleeperPartitionRegion::new(HashMap::from([region_entry(
            "key2",
            int_range(11, 25),
        )])),
        output: CompletionOptions::File {
            output_file: output.clone(),
            opts: SleeperParquetOptions::default(),
        },
        ..Default::default()
    };

    // When
    let result = run_compaction(&input).await?;

    // Then
    assert_eq!(
        read_file_of_int_fields(&output, ["key1", "key2"])?,
        vec![[9, 11], [8, 12], [7, 13], [54, 22], [23, 23], [44, 24]]
    );
    assert_eq!([result.rows_read, result.rows_written], [6, 6]);
    assert_eq!(read_sketch_min_max_ints(&sketches).await?, [11, 24]);
    Ok(())
}

#[test(tokio::test)]
async fn should_merge_empty_files() -> Result<(), Error> {
    // Given
    let dir = tempdir()?;
    let file_1 = file(&dir, "file1.parquet");
    let file_2 = file(&dir, "file2.parquet");
    let output = file(&dir, "output.parquet");
    let sketches = file(&dir, "output.sketches");
    write_file_of_ints(&file_1, "key", vec![])?;
    write_file_of_ints(&file_2, "key", vec![])?;

    let input = CommonConfig {
        input_files: Vec::from([file_1, file_2]),
        input_files_sorted: true,
        row_key_cols: row_key_cols(["key"]),
        region: SleeperPartitionRegion::new(single_int_range("key", 0, 5)),
        output: CompletionOptions::File {
            output_file: output.clone(),
            opts: SleeperParquetOptions::default(),
        },
        ..Default::default()
    };

    // When
    let result = run_compaction(&input).await?;

    // Then
    assert!(!Path::new(output.as_str()).try_exists()?);
    // IMPORTANT note that this is different to the behaviour asserted in Java, in DataFusionCompactionRunnerIT.
    // We couldn't work out how to make DataFusion output an empty file, so we added Java code to do that as an extra
    // step. That was mainly to simplify the requirements of a state store implementation, so that we don't need to
    // support a compaction that has no output file.
    // We expect this to only be temporary, as we hope that DataFusion will support outputting an empty file in the
    // following issue:
    // https://github.com/apache/datafusion/issues/16240
    // When DataFusion adds support for that, we can update this test and remove the extra behaviour from Java as well.
    assert_eq!([result.rows_read, result.rows_written], [0, 0]);
    assert_eq!(read_sketch_approx_row_count(&sketches).await?, 0);
    Ok(())
}
