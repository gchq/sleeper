//! Structs and trait to handle output of Sleeper data processing.
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
use crate::datafusion::{
    ParquetWriterConfigurer, SleeperOperations, metrics::RowCounts, util::collect_stats,
};
#[cfg(doc)]
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::{
    common::plan_err,
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{ExecutionPlan, collect, execute_stream},
    prelude::DataFrame,
};
use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};
use url::Url;

/// Defines how operation output should be given.
#[derive(Debug, Default)]
pub enum OutputType {
    /// `DataFusion` results will be returned as a stream of Arrow [`RecordBatch`]es.
    #[default]
    ArrowRecordBatch,
    /// `DataFusion` results will be written to a file with given Parquet options.
    File {
        /// Output file Url
        output_file: Url,
        /// Should a quantile sketch be written out?
        write_sketch_file: bool,
        /// Parquet output options
        opts: SleeperParquetOptions,
    },
}

impl OutputType {
    /// Create a [`Completer`] for this type of output.
    #[must_use]
    pub fn finisher<'a>(&self, ops: &'a SleeperOperations<'a>) -> Box<dyn Completer + 'a> {
        match self {
            Self::ArrowRecordBatch => Box::new(ArrowOutputCompleter::new(ops)),
            Self::File {
                output_file: _,
                write_sketch_file: _,
                opts: _,
            } => Box::new(FileOutputCompleter::new(ops)),
        }
    }
}

/// All Parquet output options supported by Sleeper.
#[derive(Debug)]
pub struct SleeperParquetOptions {
    pub max_row_group_size: usize,
    pub max_page_size: usize,
    pub compression: String,
    pub writer_version: String,
    pub column_truncate_length: usize,
    pub stats_truncate_length: usize,
    pub dict_enc_row_keys: bool,
    pub dict_enc_sort_keys: bool,
    pub dict_enc_values: bool,
}

impl Default for SleeperParquetOptions {
    fn default() -> Self {
        Self {
            max_row_group_size: 1_000_000,
            max_page_size: 65535,
            compression: "zstd".into(),
            writer_version: "v2".into(),
            column_truncate_length: usize::MAX,
            stats_truncate_length: usize::MAX,
            dict_enc_row_keys: true,
            dict_enc_sort_keys: true,
            dict_enc_values: true,
        }
    }
}

/// The result of executing a [`DataFrame`] with a [`Completer`].
///
/// A completer will return a different variant depending on the
/// [`CompletionOptions`] the plan was configured with.
pub enum CompletedOutput {
    /// Results of plan are returned as a asynchronous stream
    /// of Arrow [`RecordBatch`]es.
    ArrowRecordBatch(SendableRecordBatchStream),
    /// Results of plan have been written to a file(s) and
    /// the row counts are returned.
    File(RowCounts),
}

impl Debug for CompletedOutput {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ArrowRecordBatch(_) => write!(f, "CompletedOutput::ArrowRecordBatch"),
            Self::File(_) => write!(f, "CompletedOutput::File"),
        }
    }
}

/// A `Completer` object governs how the final stages of a Sleeper operation
/// is finished. Sometimes we want to output the results to file(s) and sometimes
/// we need a stream of processed records back.
#[async_trait]
pub trait Completer {
    /// Modify the given [`DataFrame`] as necessary for the desired output.
    ///
    /// # Errors
    /// An error will occur if the frame cannot be modified or if this [`Completer`]
    /// is not suitable for use with the configured `ops`.
    fn complete_frame(&self, frame: DataFrame) -> Result<DataFrame, DataFusionError>;
    /// Runs the plan.
    ///
    /// # Errors
    /// If any part of the conversion to physical plan or execution fails, then the error
    /// is returned.
    async fn execute_frame(
        &self,
        physical_plan: Arc<dyn ExecutionPlan>,
        task_ctx: Arc<TaskContext>,
    ) -> Result<CompletedOutput, DataFusionError>;
}

/// Writes output of frames to a Parquet file.
#[derive(Debug)]
struct FileOutputCompleter<'a> {
    ops: &'a SleeperOperations<'a>,
}

impl<'a> FileOutputCompleter<'a> {
    pub fn new(ops: &'a SleeperOperations<'a>) -> Self {
        Self { ops }
    }
}

#[async_trait]
impl Completer for FileOutputCompleter<'_> {
    fn complete_frame(&self, frame: DataFrame) -> Result<DataFrame, DataFusionError> {
        match &self.ops.config.output {
            OutputType::File {
                output_file: _,
                write_sketch_file: _,
                opts: parquet_options,
            } => {
                let configurer = ParquetWriterConfigurer { parquet_options };
                self.ops.plan_with_parquet_output(frame, &configurer)
            }
            OutputType::ArrowRecordBatch => {
                plan_err!("Can't use FileOutputCompleter with CompletionOptions::ArrowRecordBatch")
            }
        }
    }

    async fn execute_frame(
        &self,
        physical_plan: Arc<dyn ExecutionPlan>,
        task_ctx: Arc<TaskContext>,
    ) -> Result<CompletedOutput, DataFusionError> {
        collect(physical_plan.clone(), task_ctx).await?;
        let stats = collect_stats(&self.ops.config.input_files, &physical_plan)?;
        Ok(CompletedOutput::File(stats))
    }
}

/// Returns completed output as Arrow [`RecordBatch`]es.
#[derive(Debug)]
struct ArrowOutputCompleter<'a> {
    ops: &'a SleeperOperations<'a>,
}

impl<'a> ArrowOutputCompleter<'a> {
    pub fn new(ops: &'a SleeperOperations<'a>) -> Self {
        Self { ops }
    }
}

#[async_trait]
impl Completer for ArrowOutputCompleter<'_> {
    fn complete_frame(&self, frame: DataFrame) -> Result<DataFrame, DataFusionError> {
        match &self.ops.config.output {
            OutputType::File {
                output_file: _,
                write_sketch_file: _,
                opts: _,
            } => {
                plan_err!("Can't use ArrowOutputCompleter with CompletionOptions::File")
            }
            OutputType::ArrowRecordBatch => Ok(frame),
        }
    }

    async fn execute_frame(
        &self,
        physical_plan: Arc<dyn ExecutionPlan>,
        task_ctx: Arc<TaskContext>,
    ) -> Result<CompletedOutput, DataFusionError> {
        Ok(CompletedOutput::ArrowRecordBatch(execute_stream(
            physical_plan,
            task_ctx,
        )?))
    }
}
