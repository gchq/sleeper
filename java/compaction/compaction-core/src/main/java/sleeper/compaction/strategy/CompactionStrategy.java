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
package sleeper.compaction.strategy;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.statestore.FileReference;

import java.util.List;

public interface CompactionStrategy {

    default void init(InstanceProperties instanceProperties, TableProperties tableProperties) {
        init(instanceProperties, tableProperties, new CompactionJobFactory(instanceProperties, tableProperties));
    }

    void init(InstanceProperties instanceProperties, TableProperties tableProperties, CompactionJobFactory factory);

    List<CompactionJob> createCompactionJobs(List<FileReference> activeFilesWithJobId, List<FileReference> activeFilesWithNoJobId, List<Partition> allPartitions);
}
