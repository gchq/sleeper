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
