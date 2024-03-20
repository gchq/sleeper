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
package sleeper.compaction.job.creation;

import sleeper.core.table.TableStatus;

import java.util.List;

import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * Gathers failures that happened during compaction job creation. When compaction job creation fails,
 * {@link CreateCompactionJobs} will move on to the next table in the batch, and throw the exception after processing
 * the whole batch of tables.
 */
public class FailedCreateCompactionJobsException extends RuntimeException {

    private final transient List<TableFailure> tableFailures;

    public FailedCreateCompactionJobsException(List<TableFailure> tableFailures) {
        super(getMessage(tableFailures), getCause(tableFailures));
        this.tableFailures = tableFailures;
    }

    public List<TableFailure> getTableFailures() {
        return tableFailures;
    }

    private static String getMessage(List<TableFailure> tableFailures) {
        return "Compaction job creation failed for tables " +
                tableFailures.stream()
                        .map(TableFailure::getTable)
                        .collect(toUnmodifiableList());
    }

    private static Throwable getCause(List<TableFailure> tableFailures) {
        return tableFailures.stream()
                .map(TableFailure::getFailure)
                .findFirst().orElse(null);
    }

    public static class TableFailure {
        private final TableStatus table;
        private final Exception failure;

        public TableFailure(TableStatus table, Exception failure) {
            this.table = table;
            this.failure = failure;
        }

        public TableStatus getTable() {
            return table;
        }

        public Exception getFailure() {
            return failure;
        }
    }
}
