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
import java.util.function.Consumer;

/**
 * An implementation of compaction, to take a number of sorted input files and merge them into one fully sorted file.
 */
@FunctionalInterface
public interface CompactionRunner {
    /**
     * Compacts the input files of a compaction job into one output file.
     *
     * An optional progress callback can be supplied which will receive notifications of compactions progress. The rate
     * and regularity of progress updates is at the discretion of the implementation. Callbacks may receive the same row
     * count multiple times. Implementations MUST call the callback function when the compaction has finished to
     * guarantee
     * at least one update.
     *
     * <strong>Note:</strong> Callback code must be thread safe as it not specified which thread will call it, i.e. it
     * may not be the thread running the compaction that sends progress notifications.
     *
     * @param  job                       the job
     * @param  tableProperties           the configuration of the Sleeper table this takes place in
     * @param  region                    the region to be read for the compaction (usually the region of the partition
     *                                   it takes place in)
     * @param  progressCallback          callback for compaction progress notifications, may be null
     * @return                           a report of the number of rows processed
     * @throws IOException               a failure reading or writing files
     * @throws IteratorCreationException a problem creating any configured iterators
     */
    RowsProcessed compact(CompactionJob job, TableProperties tableProperties, Region region, Consumer<Long> progressCallback) throws IOException, IteratorCreationException;
}
