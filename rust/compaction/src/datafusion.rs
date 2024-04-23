/// `DataFusion` contains the implementation for performing Sleeper compactions
/// using Apache `DataFusion`.
///
/// This allows for multi-threaded compaction and optimised Parquet reading.
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
use crate::{aws_s3::ObjectStoreFactory, CompactionInput, CompactionResult};
use datafusion::{
    error::DataFusionError,
    execution::{config::SessionConfig, context::SessionContext},
};
use url::Url;

pub async fn compact(
    store_factory: &ObjectStoreFactory,
    input_data: &CompactionInput,
    input_paths: &[Url],
    output_path: &Url,
) -> Result<CompactionResult, DataFusionError> {
    let mut sf = SessionConfig::new();
    sf.options_mut().execution.parquet.data_pagesize_limit = input_data.max_page_size;
    sf.options_mut().execution.parquet.writer_version = input_data.writer_version.clone();
    sf.options_mut().execution.parquet.compression = Some(input_data.compression.clone());
    sf.options_mut().execution.parquet.max_statistics_size = Some(input_data.stats_truncate_length);
    sf.options_mut().execution.parquet.max_row_group_size = input_data.max_row_group_size;
    sf.options_mut()
        .execution
        .parquet
        .column_index_truncate_length = Some(input_data.column_truncate_length);

    // Create my scalar UDF
    let ctx = SessionContext::new_with_config(sf);

    Ok(CompactionResult {
        rows_read: 0,
        rows_written: 0,
    })
}
