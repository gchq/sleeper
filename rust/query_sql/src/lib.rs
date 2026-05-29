//! This crate contains code to allow Sleeper to have an SQL statement added to a leaf partition query that can perform
//! arbitrary SQL work on Sleeper results before returning them to the client. This enabled far more user specified
//! behaviour and flexibility than can be achieved using raw Sleeper queries.
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
mod sql_sort_fix;
use datafusion::{
    dataframe::DataFrame,
    error::DataFusionError,
    execution::context::{SQLOptions, SessionContext},
    logical_expr::SortExpr,
};
pub use sql_sort_fix::inject_sort_stage;

/// Applies an optional user-provided SQL query to the `DataFrame`.
///
/// The `DataFrame` is exposed as `my_table` for the SQL to reference.
/// Only SELECT queries are permitted.
///
/// # Errors
/// Returns an error if the SQL query is invalid or uses disallowed statements.
pub async fn add_sql_stage(
    sql: Option<&str>,
    expr: Vec<SortExpr>,
    frame: DataFrame,
    ctx: &SessionContext,
) -> Result<DataFrame, DataFusionError> {
    let Some(sql) = sql else { return Ok(frame) };
    ctx.register_table("my_table", frame.into_view())?;
    let frame = ctx
        .sql_with_options(
            sql,
            SQLOptions::new()
                .with_allow_ddl(false)
                .with_allow_dml(false)
                .with_allow_statements(false),
        )
        .await
        .map_err(|e| DataFusionError::Plan(format!("User SQL query failed: {e}")))?;
    inject_sort_stage(frame, expr)
}
