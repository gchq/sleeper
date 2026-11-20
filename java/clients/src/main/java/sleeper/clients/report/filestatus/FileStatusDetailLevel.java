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
package sleeper.clients.report.filestatus;

/**
 * The level of detail to show in a file status report.
 */
public enum FileStatusDetailLevel {

    STATS_ONLY,
    COMPACTION_JOBS,
    REFERENCES,
    FULL;

    /**
     * Whether details of all files and their references should be included.
     *
     * @return true if all file references should be shown
     */
    public boolean isShowFileReferences() {
        return this == REFERENCES || this == FULL;
    }

    /**
     * Whether current compaction job file assignments should be included.
     *
     * @return true if compaction job file assignments should be shown
     */
    public boolean isShowCompactionJobs() {
        return this == COMPACTION_JOBS || this == FULL;
    }
}
