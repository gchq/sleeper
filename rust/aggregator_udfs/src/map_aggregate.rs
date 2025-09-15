//! All APIs for performing map aggregations.
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
mod accumulator;
mod aggregator;
mod group_accumulator;
mod state;

pub use aggregator::MapAggregator;
use num_traits::NumAssign;

/// The aggregation operation to peform inside of each map. The values
/// of identical keys will be aggregated according to the specified operation.
#[derive(Debug, Clone)]
pub enum UdfMapAggregatorOp {
    Sum,
    Min,
    Max,
}

impl UdfMapAggregatorOp {
    pub fn op<T>(&self, acc: T, value: T) -> T
    where
        T: NumAssign + Ord,
    {
        match self {
            Self::Sum => acc + value,
            Self::Min => std::cmp::min(acc, value),
            Self::Max => std::cmp::max(acc, value),
        }
    }
}
