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

package sleeper.ingest.core.job;

import sleeper.core.table.TableStatus;
import sleeper.core.table.TableStatusTestHelper;

import java.util.List;

/**
 * A helper for creating ingest jobs for tests.
 */
public class IngestJobTestData {

    public static final String DEFAULT_TABLE_ID = "test-table-id";
    public static final TableStatus DEFAULT_TABLE = TableStatusTestHelper.uniqueIdAndName(DEFAULT_TABLE_ID, "test-table");

    private IngestJobTestData() {
    }

    /**
     * Creates an ingest job.
     *
     * @param  jobId     the ingest job ID
     * @param  table     the table status
     * @param  filenames a list of files to ingest
     * @return           an {@link IngestJob}
     */
    public static IngestJob createJobWithTableAndFiles(String jobId, TableStatus table, List<String> filenames) {
        return IngestJob.builder()
                .id(jobId)
                .files(filenames)
                .tableName(table.getTableName())
                .tableId(table.getTableUniqueId())
                .build();
    }

    /**
     * Creates an ingest job.
     *
     * @param  jobId     the ingest job ID
     * @param  table     the table status
     * @param  filenames the names of files to ingest
     * @return           an {@link IngestJob}
     */
    public static IngestJob createJobWithTableAndFiles(String jobId, TableStatus table, String... filenames) {
        return createJobWithTableAndFiles(jobId, table, List.of(filenames));
    }

    /**
     * Creates an ingest job. The job will be created for the table {@link IngestJobTestData#DEFAULT_TABLE}.
     *
     * @param  jobId     the ingest job ID
     * @param  filenames the names of files to ingest
     * @return           an {@link IngestJob}
     */
    public static IngestJob createJobInDefaultTable(String jobId, String... filenames) {
        return createJobWithTableAndFiles(jobId, DEFAULT_TABLE, filenames);
    }
}
