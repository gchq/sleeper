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
use crate::{
    datafusion::{sketch::serialise_sketches, udf::SketchUDF},
    details::create_sketch_path,
    s3::ObjectStoreFactory,
    store::MultipartSizeHintable,
    ColRange, CompactionInput, CompactionResult, PartitionBound,
};
use arrow::util::pretty::pretty_format_batches;
use datafusion::{
    common::{
        tree_node::{Transformed, TreeNode},
        DFSchema, DFSchemaRef,
    },
    datasource::file_format::{format_as_file_type, parquet::ParquetFormatFactory},
    error::DataFusionError,
    execution::{
        config::SessionConfig, context::SessionContext, options::ParquetReadOptions,
        FunctionRegistry,
    },
    logical_expr::{LogicalPlan, LogicalPlanBuilder, ScalarUDF, SortExpr},
    parquet::basic::{BrotliLevel, GzipLevel, ZstdLevel},
    physical_plan::{
        accept, collect, filter::FilterExec, projection::ProjectionExec, ExecutionPlan,
        ExecutionPlanVisitor,
    },
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
    prelude::*,
};
use log::{error, info, warn};
use num_format::{Locale, ToFormattedString};
use std::{collections::HashMap, iter::once, sync::Arc};
use url::Url;

pub mod sketch;
mod udf;

/// Starts a Sleeper compaction.
///
/// The object store factory must be able to produce an [`ObjectStore`] capable of reading
/// from the input URLs and writing to the output URL. A sketch file will be produced for
/// the output file.
pub async fn compact(
    store_factory: &ObjectStoreFactory,
    input_data: &CompactionInput<'_>,
    input_paths: &[Url],
    output_path: &Url,
) -> Result<CompactionResult, DataFusionError> {
    info!(
        "DataFusion compaction of files {:?}",
        input_paths.iter().map(Url::as_str).collect::<Vec<_>>()
    );
    info!("DataFusion output file {}", output_path.as_str());
    info!("Compaction partition region {:?}", input_data.region);

    let sf = create_session_cfg(input_data, input_paths);
    let ctx = SessionContext::new_with_config(sf);

    // Register some object store from first input file and output file
    let store = register_store(store_factory, input_paths, output_path, &ctx)?;

    // Find total input size and configure multipart upload size on output store
    let input_size = calculate_input_size(input_paths, &store)
        .await
        .inspect(|v| {
            info!(
                "Total input size {} bytes",
                v.to_formatted_string(&Locale::en)
            );
        })
        .inspect_err(|e| warn!("Error getting total input size {e}"));
    let multipart_size = std::cmp::max(
        crate::store::MULTIPART_BUF_SIZE,
        input_size.unwrap_or_default() / 5000,
    );
    store_factory
        .get_object_store(output_path)
        .map_err(|e| DataFusionError::External(e.into()))?
        .set_multipart_size_hint(multipart_size);
    info!(
        "Setting multipart size hint to {} bytes.",
        multipart_size.to_formatted_string(&Locale::en)
    );

    // Sort on row key columns then sort key columns (nulls last)
    let sort_order = sort_order(input_data);
    info!("Row key and sort key column order {sort_order:?}");

    // Tell DataFusion that the row key columns and sort columns are already sorted
    let po = ParquetReadOptions::default().file_sort_order(vec![sort_order.clone()]);
    let mut frame = ctx.read_parquet(input_paths.to_owned(), po).await?;

    // If we have a partition region, apply it first
    if let Some(expr) = region_filter(&input_data.region) {
        frame = frame.filter(expr)?;
    }

    // Create the sketch function
    let sketch_func = Arc::new(ScalarUDF::from(udf::SketchUDF::new(
        frame.schema(),
        &input_data.row_key_cols,
    )));
    frame.task_ctx().register_udf(sketch_func.clone())?;

    // Extract all column names
    let col_names = frame.schema().clone().strip_qualifiers().field_names();
    info!("All columns in schema {col_names:?}");

    let row_key_exprs = input_data.row_key_cols.iter().map(col).collect::<Vec<_>>();
    info!("Using sketch function {sketch_func:?}");

    let sketch_expr = once(
        sketch_func
            .call(row_key_exprs)
            .alias(&input_data.row_key_cols[0]),
    );
    // Perform sort of row key and sort key columns and projection of all columns
    let col_names_expr = sketch_expr
        .chain(col_names.iter().skip(1).map(col)) // 1st column is the sketch function call
        .collect::<Vec<_>>();

    // Store schema before applying the sketch function so we can reapply it later
    let input_schema = frame.schema().clone();

    // Build compaction query
    frame = frame.sort(sort_order)?.select(col_names_expr)?;

    // Show explanation of plan
    let explained = frame.clone().explain(false, false)?.collect().await?;
    let output = pretty_format_batches(&explained)?;
    info!("DataFusion plan:\n {output}");

    let mut pqo = ctx.copied_table_options().parquet;
    // Figure out which columns should be dictionary encoded
    let col_names = frame.schema().clone().strip_qualifiers().field_names();
    for col in &col_names {
        let col_opts = pqo.column_specific_options.entry(col.into()).or_default();
        let dict_encode = (input_data.dict_enc_row_keys && input_data.row_key_cols.contains(col))
            || (input_data.dict_enc_sort_keys && input_data.sort_key_cols.contains(col))
            // Check value columns
            || (input_data.dict_enc_values
                && !input_data.row_key_cols.contains(col)
                && !input_data.sort_key_cols.contains(col));
        col_opts.dictionary_enabled = Some(dict_encode);
    }

    // Write the frame out and collect stats
    let stats = collect_stats(frame.clone(), output_path, pqo, input_schema).await?;

    // Write sketches out to file in Sleeper compatible way
    let binding = sketch_func.inner();
    let inner_function: Option<&SketchUDF> = binding.as_any().downcast_ref();
    if let Some(func) = inner_function {
        {
            // Limit scope of MutexGuard
            let first_sketch = &func.get_sketch()[0];
            info!(
                "Made {} calls to sketch UDF and processed {} rows. Quantile sketch column 0 retained {} out of {} values (K value = {}).",
                func.get_invoke_count().to_formatted_string(&Locale::en),
                stats.rows_written.to_formatted_string(&Locale::en),
                first_sketch.get_num_retained().to_formatted_string(&Locale::en),
                first_sketch.get_n().to_formatted_string(&Locale::en),
                first_sketch.get_k().to_formatted_string(&Locale::en)
            );
        }

        // Serialise the sketch
        serialise_sketches(
            store_factory,
            &create_sketch_path(output_path),
            &func.get_sketch(),
        )
        .map_err(|e| DataFusionError::External(e.into()))?;
    }

    Ok(CompactionResult::from(&stats))
}

/// Calculate the total size of all `input_paths` objects.
///
/// The given store is queried for the size of each object.
async fn calculate_input_size(
    input_paths: &[Url],
    store: &Arc<dyn MultipartSizeHintable>,
) -> Result<usize, DataFusionError> {
    let mut total_input = 0usize;
    for path in input_paths {
        let p = path.path();
        total_input += store.head(&p.into()).await?.size;
    }
    Ok(total_input)
}

/// Write the frame out to the output path and collect statistics.
///
/// The rows read and written are returned in the [`RowCount`] object.
/// These are read from different stages in the physical plan, rows read
/// are determined by the number of filtered rows, output rows are determined
/// from the number of rows coalsced before being written.
async fn collect_stats(
    frame: DataFrame,
    output_path: &Url,
    pqo: datafusion::config::TableParquetOptions,
    schema: DFSchema,
) -> Result<RowCounts, DataFusionError> {
    // Deconstruct frame into parts, we need to do this so we can extract the physical plan before executing it.
    let task_ctx = frame.task_ctx();
    let (session_state, logical_plan) = frame.into_parts();
    let mut logical_plan = LogicalPlanBuilder::copy_to(
        logical_plan,
        output_path.as_str().into(),
        format_as_file_type(Arc::new(ParquetFormatFactory::new_with_options(pqo))),
        HashMap::default(),
        Vec::new(),
    )?
    .build()?;

    // Use a tree-node walker to change the schema in the projection node
    // to eliminate nullable columns
    logical_plan = session_state
        // Run the query optimizer
        .optimize(&logical_plan)?
        // Fix schema to remove nullable columns
        .transform(|node| {
            if let LogicalPlan::Projection(mut projection) = node {
                projection.schema = DFSchemaRef::new(schema.clone());
                return Ok(Transformed::yes(LogicalPlan::Projection(projection)));
            }
            Ok(Transformed::no(node))
        })?
        .data;

    // Convert optimised plan to physical plan
    let query_planner = DefaultPhysicalPlanner::default();
    let physical_plan = query_planner
        .create_physical_plan(&logical_plan, &session_state)
        .await?;

    let _ = collect(physical_plan.clone(), Arc::new(task_ctx)).await?;
    let mut stats = RowCounts::default();
    accept(physical_plan.as_ref(), &mut stats)?;
    Ok(stats)
}

/// Simple struct used for storing the collected statistics from an execution plan.
#[derive(Default)]
struct RowCounts {
    rows_read: usize,
    rows_written: usize,
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
        // read output records from here
        let maybe_coalesce = plan
            .as_any()
            .downcast_ref::<ProjectionExec>()
            .and_then(ExecutionPlan::metrics);
        // read input records from here
        let maybe_parq_read = plan
            .as_any()
            .downcast_ref::<FilterExec>()
            .and_then(ExecutionPlan::metrics);
        if let Some(m) = maybe_coalesce {
            self.rows_written = m.output_rows().unwrap_or_default();
        }
        if let Some(m) = maybe_parq_read {
            self.rows_read = m.output_rows().unwrap_or_default();
        }
        Ok(true)
    }
}

/// Create the `DataFusion` filtering expression from a Sleeper region.
///
/// For each column in the row keys, we look up the partition range for that
/// column and create a expression tree that combines all the various filtering conditions.
///
fn region_filter(region: &HashMap<String, ColRange>) -> Option<Expr> {
    let mut col_expr: Option<Expr> = None;
    for (name, range) in region {
        let lower_expr = lower_bound_expr(range, name);
        let upper_expr = upper_bound_expr(range, name);
        let expr = match (lower_expr, upper_expr) {
            (Some(l), Some(u)) => Some(l.and(u)),
            (Some(l), None) => Some(l),
            (None, Some(u)) => Some(u),
            (None, None) => None,
        };
        // Combine this column filter with any previous column filter
        if let Some(e) = expr {
            col_expr = match col_expr {
                Some(original) => Some(original.and(e)),
                None => Some(e),
            }
        }
    }
    col_expr
}

/// Calculate the upper bound expression on a given [`ColRange`].
///
/// This takes into account the inclusive/exclusive nature of the bound.
///
fn upper_bound_expr(range: &ColRange, name: &String) -> Option<Expr> {
    if let PartitionBound::Unbounded = range.upper {
        None
    } else {
        let max_bound = bound_to_lit_expr(&range.upper);
        if range.upper_inclusive {
            Some(col(name).lt_eq(max_bound))
        } else {
            Some(col(name).lt(max_bound))
        }
    }
}

/// Calculate the lower bound expression on a given [`ColRange`].
///
/// Not all bounds are present, so `None` is returned for the unbounded case.
///
/// This takes into account the inclusive/exclusive nature of the bound.
///
fn lower_bound_expr(range: &ColRange, name: &String) -> Option<Expr> {
    if let PartitionBound::Unbounded = range.lower {
        None
    } else {
        let min_bound = bound_to_lit_expr(&range.lower);
        if range.lower_inclusive {
            Some(col(name).gt_eq(min_bound))
        } else {
            Some(col(name).gt(min_bound))
        }
    }
}

/// Convert a [`PartitionBound`] to an [`Expr`] that can be
/// used in a bigger expression.
///
/// # Panics
/// If bound is [`PartitionBound::Unbounded`] as we can't construct
/// an expression for that.
///
fn bound_to_lit_expr(bound: &PartitionBound) -> Expr {
    match bound {
        PartitionBound::Int32(val) => lit(*val),
        PartitionBound::Int64(val) => lit(*val),
        PartitionBound::String(val) => lit(val.to_owned()),
        PartitionBound::ByteArray(val) => lit(val.to_owned()),
        PartitionBound::Unbounded => {
            error!("Can't create filter expression for unbounded partition range!");
            panic!("Can't create filter expression for unbounded partition range!");
        }
    }
}

/// Convert a Sleeper compression codec string to one `DataFusion` understands.
fn get_compression(compression: &str) -> String {
    match compression.to_lowercase().as_str() {
        x @ ("uncompressed" | "snappy" | "lzo" | "lz4") => x.into(),
        "gzip" => format!("gzip({})", GzipLevel::default().compression_level()),
        "brotli" => format!("brotli({})", BrotliLevel::default().compression_level()),
        "zstd" => format!("zstd({})", ZstdLevel::default().compression_level()),
        x => {
            error!("Unknown compression {x}, valid values: uncompressed, snappy, lzo, lz4, gzip, brotli, zstd");
            unimplemented!("Unknown compression {x}, valid values: uncompressed, snappy, lzo, lz4, gzip, brotli, zstd");
        }
    }
}

/// Convert a Sleeper Parquet version to one `DataFusion` understands.
fn get_parquet_writer_version(version: &str) -> String {
    match version {
        "v1" => "1.0".into(),
        "v2" => "2.0".into(),
        x => {
            error!("Parquet writer version invalid {x}, valid values: v1, v2");
            unimplemented!("Parquet writer version invalid {x}, valid values: v1, v2");
        }
    }
}

/// Create the `DataFusion` session configuration for a given compaction.
///
/// This sets as many parameters as possible from the given input data.
///
fn create_session_cfg<T>(input_data: &CompactionInput, input_paths: &[T]) -> SessionConfig {
    let mut sf = SessionConfig::new();
    // In order to avoid a costly "Sort" stage in the physical plan, we must make
    // sure the target partitions as at least as big as number of input files.
    sf.options_mut().execution.target_partitions =
        std::cmp::max(sf.options().execution.target_partitions, input_paths.len());
    sf.options_mut().execution.parquet.enable_page_index = false;
    sf.options_mut().execution.parquet.max_row_group_size = input_data.max_row_group_size;
    sf.options_mut().execution.parquet.data_pagesize_limit = input_data.max_page_size;
    sf.options_mut().execution.parquet.compression = Some(get_compression(&input_data.compression));
    sf.options_mut().execution.parquet.writer_version =
        get_parquet_writer_version(&input_data.writer_version);
    sf.options_mut()
        .execution
        .parquet
        .column_index_truncate_length = Some(input_data.column_truncate_length);
    sf.options_mut().execution.parquet.max_statistics_size = Some(input_data.stats_truncate_length);
    sf
}

/// Creates the sort order for a given schema.
///
/// This is a list of the row key columns followed by the sort key columns.
///
fn sort_order(input_data: &CompactionInput) -> Vec<SortExpr> {
    let sort_order = input_data
        .row_key_cols
        .iter()
        .chain(input_data.sort_key_cols.iter())
        .map(|s| col(s).sort(true, false))
        .collect::<Vec<_>>();
    sort_order
}

/// Takes the first Url in `input_paths` list and `output_path`
/// and registers the appropriate [`ObjectStore`] for it.
///
/// `DataFusion` doesn't seem to like loading a single file set from different object stores
/// so we only register the first one.
///
/// # Errors
/// If we can't create an [`ObjectStore`] for a known URL then this will fail.
///
fn register_store(
    store_factory: &ObjectStoreFactory,
    input_paths: &[Url],
    output_path: &Url,
    ctx: &SessionContext,
) -> Result<Arc<dyn MultipartSizeHintable>, DataFusionError> {
    let in_store = store_factory
        .get_object_store(&input_paths[0])
        .map_err(|e| DataFusionError::External(e.into()))?;
    ctx.runtime_env()
        .register_object_store(&input_paths[0], in_store.clone().as_object_store());
    let out_store = store_factory
        .get_object_store(output_path)
        .map_err(|e| DataFusionError::External(e.into()))?;
    ctx.runtime_env()
        .register_object_store(output_path, out_store.as_object_store());
    Ok(in_store)
}
