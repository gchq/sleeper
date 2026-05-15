//! Contains the implementation for performing Sleeper compactions
//! using Apache `DataFusion`.
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
use crate::{
    CommonConfig,
    datafusion::{
        OutputType, SleeperOperations,
        metrics::RowCounts,
        output::{CompletedOutput, Completer},
        sketch::{Sketcher, output_sketch},
        util::{explain_plan, retrieve_object_metas},
    },
    sleeper_context::SleeperContext,
};
use datafusion::{
    common::{
        plan_err,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    },
    dataframe::DataFrame,
    error::DataFusionError,
    execution::{config::SessionConfig, context::SessionContext, runtime_env::RuntimeEnv},
    physical_expr::LexOrdering,
    physical_plan::{ExecutionPlan, displayable, filter::FilterExec},
};
use log::{debug, info};
use objectstore_ext::s3::ObjectStoreFactory;
use std::sync::Arc;

/// Contains compaction results.
///
/// This provides the details of compaction results that Sleeper
/// will use to update its record keeping.
///
pub struct CompactionResult {
    /// The total number of rows read by a compaction.
    pub rows_read: usize,
    /// The total number of rows written by a compaction.
    pub rows_written: usize,
}

/// Starts a Sleeper compaction.
///
/// The object store factory must be able to produce an [`object_store::ObjectStore`] capable of reading
/// from the input URLs and writing to the output URL. A sketch file will be produced for
/// the output file.
pub async fn compact(
    store_factory: &ObjectStoreFactory,
    config: &CommonConfig<'_>,
    sleeper_context: &SleeperContext,
) -> Result<CompactionResult, DataFusionError> {
    let ops = SleeperOperations::new(config);
    info!(
        "DataFusion compaction for job ID {}: {ops}",
        config.job_id().unwrap_or(&"<unknown>".to_owned())
    );

    let runtime = sleeper_context.retrieve_runtime_env()?;

    // Retrieve Parquet output options
    let OutputType::File {
        output_file,
        write_sketch_file: _,
        opts: _,
    } = config.output()
    else {
        return plan_err!("Sleeper compactions must output to a file");
    };

    // Make compaction DataFrame
    let completer = config.output().finisher(&ops);
    let (sketcher, frame) = build_compaction_dataframe(&ops, store_factory, runtime).await?;

    let (sort_ordering, frame) = add_completion_stage(&ops, completer.as_ref(), frame)?;

    // Explain logical plan
    explain_plan(&frame).await?;

    // Run plan
    let stats = execute_compaction_plan(
        sleeper_context,
        &ops,
        completer.as_ref(),
        frame,
        sort_ordering.as_ref(),
    )
    .await?;

    // Write the frame out and collect stats
    output_sketch(store_factory, output_file, sketcher.sketch()).await?;

    // Dump input file metrics to logging console
    stats.log_metrics();
    Ok(CompactionResult::from(&stats))
}

/// Creates the [`DataFrame`] for a compaction.
///
/// This applies necessary loading, filtering, sorting, aggregation and sketch creation
/// steps to the plan.
///
/// # Errors
/// Each step of compaction may produce an error. Any are reported back to the caller.
async fn build_compaction_dataframe<'a>(
    ops: &'a SleeperOperations<'a>,
    store_factory: &ObjectStoreFactory,
    runtime: Arc<RuntimeEnv>,
) -> Result<(Sketcher<'a>, DataFrame), DataFusionError> {
    let object_metas = retrieve_object_metas(ops.config.input_files(), store_factory).await?;
    let sf = ops.apply_config(SessionConfig::new(), &object_metas)?;
    let ctx = ops
        .configure_context(
            SessionContext::new_with_config_rt(sf, runtime),
            store_factory,
        )
        .await?;
    let mut frame = ops
        .create_initial_partitioned_read(&ctx, &object_metas)
        .await?;
    frame = ops.apply_user_filters(frame)?;
    frame = ops.apply_general_sort(frame)?;
    frame = ops.apply_aggregations(frame)?;
    let sketcher = ops.create_sketcher(frame.schema());
    frame = sketcher.apply_sketch(frame)?;
    Ok((sketcher, frame))
}

fn add_completion_stage<'a>(
    ops: &'a SleeperOperations<'a>,
    completer: &(dyn Completer + 'a),
    frame: DataFrame,
) -> Result<(Option<LexOrdering>, DataFrame), DataFusionError> {
    // Create sort ordering from schema and row key and sort key columns
    let sort_ordering = ops.create_sort_expr_ordering(&frame)?;
    let frame = completer.complete_frame(frame)?;
    Ok((sort_ordering, frame))
}

/// Runs the plan in the frame.
///
/// The plan will be optimised into a physical plan, then statistics collected and returned.
///
/// # Errors
/// Any error that occurs during execution will be returned.
async fn execute_compaction_plan<'a>(
    sleeper_context: &SleeperContext,
    ops: &SleeperOperations<'_>,
    completer: &(dyn Completer + 'a),
    frame: DataFrame,
    sort_ordering: Option<&LexOrdering>,
) -> Result<RowCounts, DataFusionError> {
    let task_ctx = Arc::new(frame.task_ctx());
    let physical_plan = ops.to_physical_plan(frame, sort_ordering).await?;
    debug!(
        "Physical plan\n{}",
        displayable(physical_plan.as_ref()).indent(true)
    );

    // Put pointer to filter stage in sleeper context
    if let Some(compaction_job_id) = ops.config.job_id()
        && let Some(filter_stage) = find_filter_exec_stage(physical_plan.clone())?
    {
        sleeper_context.set_filter_stage(compaction_job_id, &filter_stage);
    }

    match completer.execute_frame(physical_plan, task_ctx).await? {
        CompletedOutput::File(stats) => Ok(stats),
        CompletedOutput::ArrowRecordBatch(_) => {
            panic!("FileOutputCompleter did not return a CompletedOutput::File")
        }
    }
}

fn find_filter_exec_stage(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
    let mut filter_exec = None;
    plan.transform_up(|node| {
        Ok(if node.as_any().downcast_ref::<FilterExec>().is_some() {
            filter_exec = Some(node.clone());
            Transformed::new(node, false, TreeNodeRecursion::Stop)
        } else {
            Transformed::new(node, false, TreeNodeRecursion::Continue)
        })
    })?;
    Ok(filter_exec)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::physical_expr::expressions::lit;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::projection::ProjectionExec;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    fn empty_input() -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(schema()))
    }

    fn filter_over(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(FilterExec::try_new(lit(true), input).expect("FilterExec should build"))
    }

    fn projection_over(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let projection_schema = input.schema();
        let exprs = projection_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                let col: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
                    datafusion::physical_expr::expressions::Column::new(field.name(), idx),
                );
                (col, field.name().to_owned())
            })
            .collect::<Vec<_>>();
        Arc::new(ProjectionExec::try_new(exprs, input).expect("ProjectionExec should build"))
    }

    #[test]
    fn find_filter_exec_stage_should_return_none_when_plan_has_no_filter() {
        // Given
        let plan = empty_input();

        // When
        let result = find_filter_exec_stage(plan).expect("traversal should succeed");

        // Then
        assert!(result.is_none());
    }

    #[test]
    fn find_filter_exec_stage_should_return_filter_when_plan_is_filter_only() {
        // Given
        let filter = filter_over(empty_input());

        // When
        let result = find_filter_exec_stage(filter.clone()).expect("traversal should succeed");

        // Then
        let found = result.expect("filter should be found");
        assert!(Arc::ptr_eq(&found, &filter));
    }

    #[test]
    fn find_filter_exec_stage_should_find_filter_nested_below_other_node() {
        // Given a Projection wrapping a Filter wrapping an EmptyExec
        let filter = filter_over(empty_input());
        let plan = projection_over(filter.clone());

        // When
        let result = find_filter_exec_stage(plan).expect("traversal should succeed");

        // Then the inner FilterExec is returned, not the Projection
        let found = result.expect("filter should be found");
        assert!(found.as_any().downcast_ref::<FilterExec>().is_some());
        assert!(Arc::ptr_eq(&found, &filter));
    }

    #[test]
    fn find_filter_exec_stage_should_return_innermost_filter_when_filters_are_nested() {
        // Given Filter(Filter(Empty))
        let inner_filter = filter_over(empty_input());
        let outer_filter = filter_over(inner_filter.clone());

        // When
        let result = find_filter_exec_stage(outer_filter).expect("traversal should succeed");

        // Then the deeper (post-order first) filter is returned
        let found = result.expect("filter should be found");
        assert!(Arc::ptr_eq(&found, &inner_filter));
    }

    #[test]
    fn find_filter_exec_stage_should_skip_filter_buried_under_other_nodes_above_first_match() {
        // Given Projection(Projection(Filter(Empty))) — only one filter, deeply nested
        let filter = filter_over(empty_input());
        let plan = projection_over(projection_over(filter.clone()));

        // When
        let result = find_filter_exec_stage(plan).expect("traversal should succeed");

        // Then the filter is still located even when wrapped in multiple non-filter nodes
        let found = result.expect("filter should be found");
        assert!(Arc::ptr_eq(&found, &filter));
    }
}
