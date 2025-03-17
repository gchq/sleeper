//! Module for collecting and logging metrics from a physical plan.
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

use datafusion::{
    common::HashMap,
    datasource::source::DataSourceExec,
    error::DataFusionError,
    physical_plan::{
        filter::FilterExec,
        metrics::{MetricValue, MetricsSet},
        projection::ProjectionExec,
        ExecutionPlan, ExecutionPlanVisitor,
    },
};
use log::info;
use std::io::Write;
use url::Url;

use crate::CompactionResult;

/// Per file metrics to collect from the Parquet reader after compaction
pub const FILE_METRICS: [&str; 10] = [
    "metadata_load_time",
    "statistics_eval_time",
    "page_index_eval_time",
    "bytes_scanned",
    "row_groups_pruned_statistics",
    "row_groups_matched_statistics",
    "row_groups_pruned_bloom_filter",
    "row_groups_matched_bloom_filter",
    "page_index_rows_pruned",
    "page_index_rows_matched",
];

/// Simple struct used for storing the collected statistics from an execution plan.
pub struct RowCounts {
    pub rows_read: usize,
    pub rows_written: usize,
    pub file_metrics: HashMap<Url, Vec<MetricValue>>,
}

impl RowCounts {
    /// Pre-populates the map of file metrics from the given list of files
    pub fn new(inputs: &[Url]) -> Self {
        Self {
            rows_read: 0,
            rows_written: 0,
            // Populate map with names
            file_metrics: inputs.iter().map(|n| (n.to_owned(), vec![])).collect(),
        }
    }
    /// Log some collected per file metrics
    pub fn log_metrics(&self) {
        for (file, metrics) in &self.file_metrics {
            let mut formatted_metrics = Vec::new();
            for m in metrics {
                write!(&mut formatted_metrics, "{} = {}, ", m.name(), m)
                    .expect("Write metrics to string failed: This is a bug");
            }
            info!(
                "File {} {{ {} }}",
                file.as_str(),
                String::from_utf8(formatted_metrics)
                    .expect("Error in UTF-8 parsing! This is a bug")
            );
        }
    }
}

impl From<&RowCounts> for CompactionResult {
    fn from(value: &RowCounts) -> Self {
        Self {
            rows_read: value.rows_read,
            rows_written: value.rows_written,
        }
    }
}

impl ExecutionPlanVisitor for RowCounts {
    type Error = DataFusionError;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        // read output records from final projection stage
        let maybe_projection = plan
            .as_any()
            .downcast_ref::<ProjectionExec>()
            .and_then(ExecutionPlan::metrics);
        // read input records from filter stage, not parquet read stage, since parquet
        // read stage may not filter precisely to range needed and therefore over-reports
        // number of rows read
        let maybe_parq_read = plan
            .as_any()
            .downcast_ref::<FilterExec>()
            .and_then(ExecutionPlan::metrics);
        if let Some(m) = maybe_projection {
            self.rows_written = m.output_rows().unwrap_or_default();
        }
        if let Some(m) = maybe_parq_read {
            self.rows_read = m.output_rows().unwrap_or_default();
        }
        // Read other metrics on a per file basis
        if let Some(m) = plan
            .as_any()
            .downcast_ref::<DataSourceExec>()
            .and_then(ExecutionPlan::metrics)
        {
            for (file, file_metrics) in &mut self.file_metrics {
                for metric_name in FILE_METRICS {
                    if let Some(val) = metric_sum_by_filename(&m, metric_name, file.as_str()) {
                        file_metrics.push(val);
                    }
                }
            }
        }
        Ok(true)
    }
}

/// Sum all metrics of a given name for a given filename.
pub fn metric_sum_by_filename(
    metrics: &MetricsSet,
    name: &str,
    filename: &str,
) -> Option<MetricValue> {
    metrics.sum(|m| {
        m.value().name() == name
            && m.labels()
                .iter()
                .any(|label| label.name() == "filename" && filename.ends_with(label.value()))
    })
}
