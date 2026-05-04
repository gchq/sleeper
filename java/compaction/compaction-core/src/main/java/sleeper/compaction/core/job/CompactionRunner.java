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
package sleeper.compaction.core.job;

import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Region;
import sleeper.core.tracker.job.run.RowsProcessed;

import java.io.IOException;

/**
 * An implementation of compaction, to take a number of sorted input files and merge them into one fully sorted file.
 */
public interface CompactionRunner {
    /**
     * Compacts the input files of a compaction job into one output file.
     *
     * @param  job                       the job
     * @param  tableProperties           the configuration of the Sleeper table this takes place in
     * @param  region                    the region to be read for the compaction (usually the region of the partition
     *                                   it takes place in)
     * @return                           a report of the number of rows processed
     * @throws IOException               a failure reading or writing files
     * @throws IteratorCreationException a problem creating any configured iterators
     */
    RowsProcessed compact(CompactionJob job, TableProperties tableProperties, Region region) throws IOException, IteratorCreationException;

    /**
     * Gets whether a compaction is in progress.
     *
     * @implNote this method MUST be thread safe! It must be safe to call this method while another thread is
     *           executing
     *           {@link CompactionRunner#compact(CompactionJob, TableProperties, Region)}.
     *
     * @return   true if a compaction has been started, but not yet finished
     */
    boolean isCompactionInProgress();

    /**
     * Gets the number of input rows read by a currently in progress compaction.
     *
     * The compactor should regularly update the count of the number of rows read during a compaction. By polling this
     * method,
     * it is possible to determine if a compaction is making progress.
     *
     * @implNote                       this method MUST be thread safe! It must be safe to call this method while
     *                                 another thread is executing
     *                                 {@link CompactionRunner#compact(CompactionJob, TableProperties, Region)}.
     *
     * @return                         the number of rows read by the currently executing compaction
     * @throws   IllegalStateException if this method is called when {@link CompactionRunner#isCompactionInProgress()}
     *                                 returns false
     */
    long getRowsReadByCurrentCompaction() throws IllegalStateException;
}
