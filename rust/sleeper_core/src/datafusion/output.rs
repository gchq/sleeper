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
use crate::{
    SleeperParquetOptions,
    datafusion::{SleeperOperations, metrics::RowCounts},
};
use datafusion::{
    error::DataFusionError, execution::SendableRecordBatchStream, prelude::DataFrame,
};
use std::fmt::{Debug, Formatter};
use url::Url;

/// Defines how operation output should be given.
#[derive(Debug, Default)]
pub enum OperationOutput {
    /// `DataFusion` results will be returned as a stream of Arrow [`RecordBatch`]es.
    #[default]
    ArrowRecordBatch,
    /// `DataFusion` results will be written to a file with given Parquet options.
    File {
        /// Output file Url
        output_file: Url,
        /// Parquet output options
        opts: SleeperParquetOptions,
    },
}

// impl OperationOutput {
//     /// Create a [`Completer`] for this type of output.
//     pub fn finisher(&self) -> Arc<dyn Completer> {
//         Arc::new(match self {
//             Self::ArrowRecordBatch => {
//                 unimplemented!()
//             }
//             Self::File { output_file, opts } => {
//                 unimplemented!()
//             }
//         })
//     }
// }

pub enum CompletedOutput {
    ArrowRecordBatch(SendableRecordBatchStream),
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

pub trait Completer {
    fn complete(
        ops: &SleeperOperations<'_>,
        frame: DataFrame,
    ) -> Result<CompletedOutput, DataFusionError>;
}

// #[derive(Debug)]
// pub struct RecordBatchCompleter {}

// impl Completer for RecordBatchCompleter {}

#[derive(Debug)]
pub struct FileOutputCompleter {}

impl FileOutputCompleter {}

impl Completer for FileOutputCompleter {
    fn complete(
        ops: &SleeperOperations<'_>,
        frame: DataFrame,
    ) -> Result<CompletedOutput, DataFusionError> {
        todo!()
    }
}
