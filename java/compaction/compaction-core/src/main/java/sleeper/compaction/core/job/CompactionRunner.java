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
import sleeper.core.tracker.job.run.RowsProcessed;

import java.io.IOException;

/**
 * An implementation of compaction, to take a number of sorted input files and merge them into one fully sorted file.
 */
@FunctionalInterface
public interface CompactionRunner {
    /**
     * Compacts the input files of a compaction job into one output file.
     *
     * The supplied {@link CompactionRequest} carries the job, the table configuration, the region to read, and a
     * progress callback. The progress callback is always non-null: if the caller does not specify one, the request will
     * carry a no-op consumer. The rate and regularity of progress updates is at the discretion of the implementation.
     * Callbacks may receive the same row count multiple times. Implementations MUST call the callback function when the
     * compaction has finished to guarantee at least one update.
     *
     * <strong>Note:</strong> Callback code must be thread safe as it is not specified which thread will call it, i.e.
     * it may not be the thread running the compaction that sends progress notifications.
     *
     * @param  request                   compaction request details
     * @return                           a report of the number of rows processed
     * @throws IOException               a failure reading or writing files
     * @throws IteratorCreationException a problem creating any configured iterators
     */
    RowsProcessed compact(CompactionRequest request) throws IOException, IteratorCreationException;
}
