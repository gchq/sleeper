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
package sleeper.compaction.core.strategy;

import sleeper.compaction.core.strategy.CompactionStrategyIndex.FilesInPartition;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

public interface ShouldCreateJobsStrategy {

    default void init(InstanceProperties instanceProperties, TableProperties tableProperties) {
    }

    long maxCompactionJobsToCreate(FilesInPartition filesInPartition);

    static ShouldCreateJobsStrategy yes() {
        return (filesInPartition) -> Long.MAX_VALUE;
    }
}
