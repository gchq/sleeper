/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.ingest.job;

import sleeper.core.table.TableIdentity;

import java.util.List;

public class IngestJobTestData {

    public static final TableIdentity DEFAULT_TABLE = TableIdentity.uniqueIdAndName("test-table-id", "test-table");
    public static final String DEFAULT_TABLE_NAME = DEFAULT_TABLE.getTableName();

    private IngestJobTestData() {
    }

    public static IngestJob createJobWithTableAndFiles(String jobId, TableIdentity table, List<String> filenames) {
        return IngestJob.builder()
                .id(jobId)
                .files(filenames)
                .tableName(table.getTableName())
                .tableId(table.getTableUniqueId())
                .build();
    }

    public static IngestJob createJobWithTableAndFiles(String jobId, TableIdentity table, String... filenames) {
        return createJobWithTableAndFiles(jobId, table, List.of(filenames));
    }

    public static IngestJob createJobInDefaultTable(String jobId, String... filenames) {
        return createJobWithTableAndFiles(jobId, DEFAULT_TABLE, filenames);
    }
}
