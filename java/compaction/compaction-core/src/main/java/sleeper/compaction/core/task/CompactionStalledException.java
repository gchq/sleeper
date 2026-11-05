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
package sleeper.compaction.core.task;

/**
 * Thrown when a compaction returns after it was detected as stalled. The job has already been returned to the queue
 * and the task has been told to terminate, so no further work should be done.
 */
public class CompactionStalledException extends RuntimeException {

    public CompactionStalledException(String jobId, CompactionStall stall) {
        super("Compaction job " + jobId + " stalled: " + stall.description());
    }
}
