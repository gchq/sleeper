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
package sleeper.core.statestore;

import sleeper.core.statestore.exception.ReplaceRequestsFailedException;

import java.util.List;

/**
 * Handles updates to the data files in a Sleeper table. This includes a count of the number of references
 * to the file, and internal references which assign all the data in the file to non-overlapping partitions.
 */
public interface FileReferenceStoreUpdates {

    /**
     * Atomically applies the results of jobs. Removes file references for a job's input files, and adds a reference to
     * an output file. This will be used for compaction.
     * <p>
     * This will validate that the input files were assigned to the job.
     * <p>
     * This will decrement the number of references for each of the input files. If no other references exist for those
     * files, they will become available for garbage collection.
     *
     * @param  requests                       requests for jobs to each have their results atomically applied
     * @throws ReplaceRequestsFailedException if any of the updates fail
     */
    void atomicallyReplaceFileReferencesWithNewOnes(List<ReplaceFileReferencesRequest> requests) throws ReplaceRequestsFailedException;

}
