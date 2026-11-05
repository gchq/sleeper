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
 * A helper for creating compaction progress for tests.
 */
public class CompactionProgressTestData {

    private CompactionProgressTestData() {
    }

    /**
     * Creates compaction progress, naming the parameters in the order they are given.
     *
     * @param  rowsProcessed  the number of rows reported in the latest update
     * @param  lastUpdateTime the time the latest update was received
     * @param  lastChangeTime the time the number of rows last changed
     * @return                the progress
     */
    public static CompactionProgress rowsProcessedLastUpdateAndLastChange(long rowsProcessed, String lastUpdateTime, String lastChangeTime) {
        return new CompactionProgress(rowsProcessed, Instant.parse(lastUpdateTime), Instant.parse(lastChangeTime));
    }
}
