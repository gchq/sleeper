//! Context module for Sleeper. Allows certain contextual information to be persisted in an outer FFI context
//! object. This avoids re-creating some internal state with each query/compaction operation.
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
use crate::{AwsConfig, CommonConfig};
use std::sync::Mutex;

/// A thread-safe class containing internal DataFusion context that needs to be
/// persisted between queries/compactions.
#[derive(Debug, Default)]
pub struct SleeperContext {
    inner: Mutex<State>,
}

/// Inner state that can be mutated behind a lock
#[derive(Debug)]
struct State {
    aws_config: Option<AwsConfig>,
    use_readahead: bool,
}

impl SleeperContext {
    pub fn should_recreate_context(&self, common_config: &CommonConfig) -> bool {
        let guard = self.inner.lock().expect("SleeperContext lock poisoned");
        guard.use_readahead == common_config.use_readahead_store
            && guard.aws_config == common_config.aws_config
    }
}
