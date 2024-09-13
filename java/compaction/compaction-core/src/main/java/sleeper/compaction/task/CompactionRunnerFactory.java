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
package sleeper.compaction.task;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionRunner;
import sleeper.configuration.properties.table.TableProperties;

/**
 * Interface for classes that implement logic for choosing which compaction method should be chosen.
 */
@FunctionalInterface
public interface CompactionRunnerFactory {

    /**
     * Picks a CompactionRunner implementation that is capable
     * of running a compaction on the given job.
     *
     * @param  job             the job
     * @param  tableProperties the Sleeper table properties
     * @return                 a compaction runner
     */
    default CompactionRunner createCompactor(CompactionJob job, TableProperties tableProperties) {
        return createCompactor(job);
    }

    /**
     * Picks a CompactionRunner implementation that is capable
     * of running a compaction on the given job.
     *
     * @param  job the job
     * @return     a compaction runner
     */
    CompactionRunner createCompactor(CompactionJob job);
}
