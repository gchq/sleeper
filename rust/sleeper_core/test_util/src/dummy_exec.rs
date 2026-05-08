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
use datafusion::{
    error::Result,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{
        DisplayAs, ExecutionPlan, PlanProperties,
        metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
    },
};
use std::{any::Any, sync::Arc};

#[derive(Debug)]
pub struct DummyExec(ExecutionPlanMetricsSet, Count);

impl DisplayAs for DummyExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        _f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        unimplemented!();
    }
}

impl Default for DummyExec {
    fn default() -> Self {
        let metric_set = ExecutionPlanMetricsSet::new();
        let output_rows = MetricBuilder::new(&metric_set).output_rows(0);
        Self(metric_set, output_rows)
    }
}

impl DummyExec {
    pub fn add_output_rows(&self, rows: usize) {
        self.1.add(rows);
    }
}

impl ExecutionPlan for DummyExec {
    fn name(&self) -> &'static str {
        "DummyExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        unimplemented!()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        unimplemented!()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.0.clone_inner())
    }
}
