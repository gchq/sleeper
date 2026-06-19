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
    sql: &str,
    expr: Vec<SortExpr>,
    frame: DataFrame,
    ctx: &SessionContext,
) -> Result<DataFrame, DataFusionError> {
    ctx.register_table("query_results", frame.into_view())?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{
        arrow::{
            array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray},
            datatypes::{DataType, Field, Schema},
        },
        logical_expr::{LogicalPlan, col},
    };
    use std::sync::Arc;

    fn create_test_context() -> SessionContext {
        SessionContext::new()
    }

    fn create_test_dataframe(ctx: &SessionContext) -> DataFrame {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![3, 1, 4, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec!["foo", "bar", "baz", "qux"])) as ArrayRef,
            ],
        )
        .unwrap();
        ctx.read_batch(batch).unwrap()
    }

    fn create_sort_exprs_single() -> Vec<SortExpr> {
        vec![col("a").sort(true, true)]
    }

    fn create_sort_exprs_multiple() -> Vec<SortExpr> {
        vec![col("a").sort(true, true), col("b").sort(false, false)]
    }

    fn logical_plans_equal(plan1: &LogicalPlan, plan2: &LogicalPlan) -> bool {
        if std::mem::discriminant(plan1) != std::mem::discriminant(plan2) {
            return false;
        }
        if plan1.inputs().len() != plan2.inputs().len() {
            return false;
        }
        if plan1.schema() != plan2.schema() {
            return false;
        }
        plan1
            .inputs()
            .iter()
            .zip(plan2.inputs().iter())
            .all(|(i1, i2)| logical_plans_equal(i1, i2))
    }

    #[tokio::test]
    async fn should_return_unchanged_dataframe_when_sql_is_none() {
        // Given
        let ctx = create_test_context();
        let frame = create_test_dataframe(&ctx);
        let sort_exprs = create_sort_exprs_single();
        let (_, original_plan) = frame.clone().into_parts();

        // When
        let result = add_sql_stage(None, sort_exprs, frame, &ctx).await.unwrap();

        // Then
        let (_, result_plan) = result.into_parts();
        assert!(
            logical_plans_equal(&original_plan, &result_plan),
            "Logical plans should be identical when SQL is None"
        );
    }

    #[tokio::test]
    async fn should_execute_simple_select_query_with_empty_sort_expr() {
        // Given
        let ctx = create_test_context();
        let frame = create_test_dataframe(&ctx);
        let sort_exprs: Vec<SortExpr> = vec![];

        // When
        let result = add_sql_stage(Some("SELECT * FROM query_results"), sort_exprs, frame, &ctx)
            .await
            .unwrap();

        // Then
        let batches = result.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 4);
    }

    #[tokio::test]
    async fn should_execute_simple_select_query_with_one_sort_expr() {
        // Given
        let ctx = create_test_context();
        let frame = create_test_dataframe(&ctx);
        let sort_exprs = create_sort_exprs_single();

        // When
        let result = add_sql_stage(Some("SELECT * FROM query_results"), sort_exprs, frame, &ctx)
            .await
            .unwrap();

        // Then
        let batches = result.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 4);
    }

    #[tokio::test]
    async fn should_execute_simple_select_query_with_two_sort_exprs() {
        // Given
        let ctx = create_test_context();
        let frame = create_test_dataframe(&ctx);
        let sort_exprs = create_sort_exprs_multiple();

        // When
        let result = add_sql_stage(Some("SELECT * FROM query_results"), sort_exprs, frame, &ctx)
            .await
            .unwrap();

        // Then
        let batches = result.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 4);
    }

    #[tokio::test]
    async fn should_filter_data_with_where_clause() {
        // Given
        let ctx = create_test_context();
        let frame = create_test_dataframe(&ctx);
        let sort_exprs = create_sort_exprs_single();

        // When
        let result = add_sql_stage(
            Some("SELECT * FROM query_results WHERE a > 2"),
            sort_exprs,
            frame,
            &ctx,
        )
        .await
        .unwrap();

        // Then
        let batches = result.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        let a_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(a_col.value(0), 3);
        assert_eq!(a_col.value(1), 4);
    }

    #[tokio::test]
    async fn should_project_columns_with_select_list() {
        // Given
        let ctx = create_test_context();
        let frame = create_test_dataframe(&ctx);
        let sort_exprs = create_sort_exprs_single();

        // When
        let result = add_sql_stage(Some("SELECT a FROM query_results"), sort_exprs, frame, &ctx)
            .await
            .unwrap();

        // Then
        let batches = result.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "a");
    }

    #[tokio::test]
    async fn should_apply_group_by_with_aggregation() {
        // Given
        let ctx = create_test_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["A", "B", "A", "B"])) as ArrayRef,
                Arc::new(Int32Array::from(vec![10, 20, 30, 40])) as ArrayRef,
            ],
        )
        .unwrap();
        let frame = ctx.read_batch(batch).unwrap();
        let sort_exprs = vec![col("category").sort(true, true)];

        // When
        let result = add_sql_stage(
            Some("SELECT category, SUM(value) as total FROM query_results GROUP BY category"),
            sort_exprs,
            frame,
            &ctx,
        )
        .await
        .unwrap();

        // Then
        let batches = result.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 2, "Expected 2 rows total across all batches");

        let mut all_results: Vec<(String, i64)> = Vec::new();
        for batch in &batches {
            assert_eq!(batch.num_columns(), 2);
            let category_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let total_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            for i in 0..batch.num_rows() {
                all_results.push((category_col.value(i).to_string(), total_col.value(i)));
            }
        }
        all_results.sort();

        assert_eq!(
            all_results,
            vec![("A".to_string(), 40), ("B".to_string(), 60)]
        );
    }

    #[tokio::test]
    async fn should_apply_reverse_sort_with_order_by_desc() {
        // Given
        let ctx = create_test_context();
        let frame = create_test_dataframe(&ctx);
        let sort_exprs = vec![];

        // When
        let result = add_sql_stage(
            Some("SELECT * FROM query_results ORDER BY a DESC"),
            sort_exprs,
            frame,
            &ctx,
        )
        .await
        .unwrap();

        // Then
        let batches = result.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        let a_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(a_col.value(0), 4);
        assert_eq!(a_col.value(1), 3);
        assert_eq!(a_col.value(2), 2);
        assert_eq!(a_col.value(3), 1);
    }

    #[tokio::test]
    async fn should_error_on_invalid_sql_syntax() {
        // Given
        let ctx = create_test_context();
        let frame = create_test_dataframe(&ctx);
        let sort_exprs = vec![];

        // When
        let result = add_sql_stage(
            Some("THIS IS NOT VALID SQL AT ALL!!!"),
            sort_exprs,
            frame,
            &ctx,
        )
        .await;

        // Then
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("User SQL query failed"));
    }

    #[tokio::test]
    async fn should_error_on_ddl_create_table() {
        // Given
        let ctx = create_test_context();
        let frame = create_test_dataframe(&ctx);
        let sort_exprs = vec![];

        // When
        let result = add_sql_stage(
            Some("CREATE TABLE new_table (id INT)"),
            sort_exprs,
            frame,
            &ctx,
        )
        .await;

        // Then
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("User SQL query failed"));
    }

    #[tokio::test]
    async fn should_error_on_ddl_drop_table() {
        // Given
        let ctx = create_test_context();
        let frame = create_test_dataframe(&ctx);
        let sort_exprs = vec![];

        // When
        let result = add_sql_stage(Some("DROP TABLE query_results"), sort_exprs, frame, &ctx).await;

        // Then
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("User SQL query failed"));
    }

    #[tokio::test]
    async fn should_error_on_ddl_alter_table() {
        // Given
        let ctx = create_test_context();
        let frame = create_test_dataframe(&ctx);
        let sort_exprs = vec![];

        // When
        let result = add_sql_stage(
            Some("ALTER TABLE query_results ADD COLUMN c INT"),
            sort_exprs,
            frame,
            &ctx,
        )
        .await;

        // Then
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("User SQL query failed"));
    }

    #[tokio::test]
    async fn should_error_on_dml_insert() {
        // Given
        let ctx = create_test_context();
        let frame = create_test_dataframe(&ctx);
        let sort_exprs = vec![];

        // When
        let result = add_sql_stage(
            Some("INSERT INTO query_results VALUES (5, 'test')"),
            sort_exprs,
            frame,
            &ctx,
        )
        .await;

        // Then
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("User SQL query failed"));
    }

    #[tokio::test]
    async fn should_error_on_dml_update() {
        // Given
        let ctx = create_test_context();
        let frame = create_test_dataframe(&ctx);
        let sort_exprs = vec![];

        // When
        let result = add_sql_stage(
            Some("UPDATE query_results SET a = 10"),
            sort_exprs,
            frame,
            &ctx,
        )
        .await;

        // Then
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("User SQL query failed"));
    }

    #[tokio::test]
    async fn should_error_on_dml_delete() {
        // Given
        let ctx = create_test_context();
        let frame = create_test_dataframe(&ctx);
        let sort_exprs = vec![];

        // When
        let result = add_sql_stage(
            Some("DELETE FROM query_results WHERE a = 1"),
            sort_exprs,
            frame,
            &ctx,
        )
        .await;

        // Then
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("User SQL query failed"));
    }

    #[tokio::test]
    async fn should_error_on_nonexistent_table_reference() {
        // Given
        let ctx = create_test_context();
        let frame = create_test_dataframe(&ctx);
        let sort_exprs = vec![];

        // When
        let result =
            add_sql_stage(Some("SELECT * FROM wrong_table"), sort_exprs, frame, &ctx).await;

        // Then
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("User SQL query failed"));
    }

    #[tokio::test]
    async fn should_error_on_nonexistent_column() {
        // Given
        let ctx = create_test_context();
        let frame = create_test_dataframe(&ctx);
        let sort_exprs = vec![];

        // When
        let result = add_sql_stage(
            Some("SELECT nonexistent_column FROM query_results"),
            sort_exprs,
            frame,
            &ctx,
        )
        .await;

        // Then
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("User SQL query failed"));
    }
}
