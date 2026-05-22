//! Context module for Sleeper. Allows certain contextual information to be persisted in an outer FFI context
//! object. This avoids re-creating some internal state with each query/compaction operation.
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
#[cfg(doc)]
use datafusion::execution::object_store::ObjectStoreRegistry;
use datafusion::{
    common::HashMap,
    error::DataFusionError,
    execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
    physical_plan::{ExecutionPlan, metrics::MetricsSet},
};
use std::sync::{Arc, Mutex, Weak};

/// A thread-safe class containing internal `DataFusion` context that needs to be
/// persisted between queries/compactions.
#[derive(Debug, Default)]
pub struct SleeperContext {
    // Interior mutable runtime
    inner: Mutex<RuntimeEnv>,
    // Weak pointers to currently executing compactions
    compaction_filter_stages: Mutex<HashMap<String, Weak<dyn ExecutionPlan>>>,
}

/// The maximum size of `DataFusion`'s file metadata cache.
pub const METADATA_CACHE_SIZE: usize = 128 * 1024 * 1024;

impl SleeperContext {
    /// Retrieves a runtime environment for `DataFusion` to work with.
    ///
    /// The previous runtime is cloned and the [`ObjectStoreRegistry`] replaced to prevent object stores
    /// being cached across queries/compactions.
    ///
    /// # Panics
    /// If the lock protecting the runtime can't be unlocked due to a lock posion.
    ///
    /// # Errors
    /// An error results if the runtime can't be built
    pub fn retrieve_runtime_env(&self) -> Result<Arc<RuntimeEnv>, DataFusionError> {
        let guard = self.inner.lock().expect("SleeperContext lock poisoned");
        Ok(Arc::new(
            RuntimeEnvBuilder::from_runtime_env(&guard)
                .with_object_store_registry(
                    RuntimeEnvBuilder::default().object_store_registry.clone(),
                )
                .with_metadata_cache_limit(METADATA_CACHE_SIZE)
                .build()?,
        ))
    }

    /// Retrieves number of compaction rows read for given compaction job.
    ///
    /// If this compaction job id is recognised, then the number of input rows read from it
    /// is returned. This function will only return results from currently executing compactions.
    ///
    /// # Panics
    /// If the lock protecting the runtime can't be unlocked due to a lock posion.
    pub fn get_compaction_rows_read(&self, compaction_job_id: impl AsRef<str>) -> Option<usize> {
        let plan = {
            let mut guard = self
                .compaction_filter_stages
                .lock()
                .expect("SleeperContext lock poisoned");

            Self::purge_dropped_values(&mut guard);

            guard.get(compaction_job_id.as_ref())?.upgrade()?
        };

        plan.metrics().as_ref().and_then(MetricsSet::output_rows)
    }

    fn purge_dropped_values(map: &mut HashMap<String, Weak<dyn ExecutionPlan>>) {
        map.retain(|_, weak| weak.strong_count() > 0);
    }

    /// Insert a filter stage from a compaction into this context.
    ///
    /// The filter stage is stored using a weak pointer so it won't prevent dropping of the
    /// stage when the reference count drops to zero.
    ///
    /// # Panics
    /// If the lock protecting the runtime can't be unlocked due to a lock posion.
    pub fn set_filter_stage(
        &self,
        compaction_job_id: impl Into<String>,
        filter_plan_stage: &Arc<dyn ExecutionPlan>,
    ) {
        self.compaction_filter_stages
            .lock()
            .expect("SleeperContext lock poisoned")
            .insert(compaction_job_id.into(), Arc::downgrade(filter_plan_stage));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use datafusion::physical_plan::empty::EmptyExec;

    fn make_plan() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            arrow::datatypes::DataType::Int32,
            false,
        )]));
        Arc::new(EmptyExec::new(schema))
    }

    #[test]
    fn set_filter_stage_should_insert_weak_pointer_keyed_by_job_id() {
        // Given
        let context = SleeperContext::default();
        let plan = make_plan();

        // When
        context.set_filter_stage("job-1", &plan);

        // Then
        let guard = context.compaction_filter_stages.lock().unwrap();
        assert_eq!(guard.len(), 1);
        let weak = guard.get("job-1").expect("entry should exist");
        let upgraded = weak.upgrade().expect("plan should still be alive");
        assert!(Arc::ptr_eq(&upgraded, &plan));
    }

    #[test]
    fn set_filter_stage_should_overwrite_existing_entry_for_same_job_id() {
        // Given
        let context = SleeperContext::default();
        let first = make_plan();
        let second = make_plan();

        // When
        context.set_filter_stage("job-1", &first);
        context.set_filter_stage("job-1", &second);

        // Then
        let guard = context.compaction_filter_stages.lock().unwrap();
        assert_eq!(guard.len(), 1);
        let upgraded = guard.get("job-1").unwrap().upgrade().unwrap();
        assert!(Arc::ptr_eq(&upgraded, &second));
        assert!(!Arc::ptr_eq(&upgraded, &first));
    }

    #[test]
    fn set_filter_stage_should_not_keep_plan_alive() {
        // Given
        let context = SleeperContext::default();
        let plan = make_plan();
        context.set_filter_stage("job-1", &plan);

        // When
        drop(plan);

        // Then
        let guard = context.compaction_filter_stages.lock().unwrap();
        assert!(
            guard.get("job-1").unwrap().upgrade().is_none(),
            "weak pointer should not keep plan alive after last strong ref drops"
        );
    }

    #[test]
    fn set_filter_stage_should_accept_multiple_distinct_jobs() {
        // Given
        let context = SleeperContext::default();
        let plan_a = make_plan();
        let plan_b = make_plan();

        // When
        context.set_filter_stage("job-a", &plan_a);
        context.set_filter_stage(String::from("job-b"), &plan_b);

        // Then
        let guard = context.compaction_filter_stages.lock().unwrap();
        assert_eq!(guard.len(), 2);
        assert!(guard.contains_key("job-a"));
        assert!(guard.contains_key("job-b"));
    }

    #[test]
    fn purge_dropped_values_should_remove_entries_whose_plan_has_been_dropped() {
        // Given
        let mut map: HashMap<String, Weak<dyn ExecutionPlan>> = HashMap::new();
        let live = make_plan();
        let to_drop = make_plan();
        map.insert("live".to_owned(), Arc::downgrade(&live));
        map.insert("to_drop".to_owned(), Arc::downgrade(&to_drop));
        drop(to_drop);

        // When
        SleeperContext::purge_dropped_values(&mut map);

        // Then
        assert_eq!(map.len(), 1);
        assert!(map.contains_key("live"));
        assert!(!map.contains_key("to_drop"));
    }

    #[test]
    fn purge_dropped_values_should_be_a_noop_when_all_plans_are_alive() {
        // Given
        let mut map: HashMap<String, Weak<dyn ExecutionPlan>> = HashMap::new();
        let a = make_plan();
        let b = make_plan();
        map.insert("a".to_owned(), Arc::downgrade(&a));
        map.insert("b".to_owned(), Arc::downgrade(&b));

        // When
        SleeperContext::purge_dropped_values(&mut map);

        // Then
        assert_eq!(map.len(), 2);
        assert!(Arc::ptr_eq(&a, &map.get("a").unwrap().upgrade().unwrap()));
        assert!(Arc::ptr_eq(&b, &map.get("b").unwrap().upgrade().unwrap()));
    }

    #[test]
    fn purge_dropped_values_should_clear_map_when_all_plans_have_been_dropped() {
        // Given
        let mut map: HashMap<String, Weak<dyn ExecutionPlan>> = HashMap::new();
        {
            let a = make_plan();
            let b = make_plan();
            map.insert("a".to_owned(), Arc::downgrade(&a));
            map.insert("b".to_owned(), Arc::downgrade(&b));
        }

        // When
        SleeperContext::purge_dropped_values(&mut map);

        // Then
        assert!(map.is_empty());
    }

    #[test]
    fn purge_dropped_values_should_handle_an_empty_map() {
        // Given
        let mut map: HashMap<String, Weak<dyn ExecutionPlan>> = HashMap::new();

        // When
        SleeperContext::purge_dropped_values(&mut map);

        // Then
        assert!(map.is_empty());
    }
}
