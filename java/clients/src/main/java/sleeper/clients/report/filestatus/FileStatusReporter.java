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
 * A reporter to present the status of files in a Sleeper table to a user.
 */
public interface FileStatusReporter {

    /**
     * Writes a report on the status of files.
     *
     * @param status      the status of files in the Sleeper table
     * @param detailLevel the level of detail to include
     */
    void report(TableFilesStatus status, FileStatusDetailLevel detailLevel);
}
