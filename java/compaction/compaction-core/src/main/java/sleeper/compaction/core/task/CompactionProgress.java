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

import java.time.Instant;

/**
 * The latest progress update received from a running compaction.
 *
 * @param rowsProcessed  the number of rows reported in the latest update
 * @param lastUpdateTime the time the latest update was received
 * @param lastChangeTime the time the number of rows last changed
 */
public record CompactionProgress(long rowsProcessed, Instant lastUpdateTime, Instant lastChangeTime) {

    /**
     * Creates the progress for the first update received from a compaction.
     *
     * @param  rowsProcessed the number of rows reported
     * @param  time          the time the update was received
     * @return               the progress
     */
    public static CompactionProgress firstUpdate(long rowsProcessed, Instant time) {
        return new CompactionProgress(rowsProcessed, time, time);
    }

    /**
     * Applies a new progress update. The change time only moves if the number of rows is different. Any change counts
     * as progress, as the final update may report rows written rather than rows read, which can be lower.
     *
     * @param  newRowsProcessed the number of rows reported
     * @param  time             the time the update was received
     * @return                  the updated progress
     */
    public CompactionProgress update(long newRowsProcessed, Instant time) {
        Instant changeTime = newRowsProcessed == rowsProcessed ? lastChangeTime : time;
        return new CompactionProgress(newRowsProcessed, time, changeTime);
    }
}
