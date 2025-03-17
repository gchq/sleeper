/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.garbagecollector;

import sleeper.core.table.TableStatus;

import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * Gathers failures that happened during garbage collection, to be thrown after processing a batch. When GC fails, the
 * {@link GarbageCollector} will continue to collect any remaining files and tables, and will throw this exception after
 * attempting to collect all eligible files in all tables it was invoked for.
 */
public class FailedGarbageCollectionException extends RuntimeException {

    private final transient List<TableFailures> tableFailures;

    public FailedGarbageCollectionException(List<TableFailures> tableFailures) {
        super(getMessage(tableFailures), getCause(tableFailures));
        this.tableFailures = tableFailures;
    }

    private static String getMessage(List<TableFailures> tableFailures) {
        return "Found garbage collection failures for tables: " +
                tableFailures.stream().map(TableFailures::getTable).collect(toUnmodifiableList());
    }

    private static Exception getCause(List<TableFailures> tableFailures) {
        return tableFailures.stream()
                .flatMap(table -> table.streamFailures())
                .findFirst().orElse(null);
    }

    public List<TableFailures> getTableFailures() {
        return tableFailures;
    }

    public static class TableFailures {
        private final TableStatus table;
        private final Exception tableFailure;
        private final List<FileFailure> fileFailures;
        private final List<StateStoreUpdateFailure> stateStoreUpdateFailures;

        public TableFailures(
                TableStatus table, Exception tableFailure,
                List<FileFailure> fileFailures,
                List<StateStoreUpdateFailure> stateStoreUpdateFailures) {
            this.table = table;
            this.tableFailure = tableFailure;
            this.fileFailures = fileFailures;
            this.stateStoreUpdateFailures = stateStoreUpdateFailures;
        }

        public TableStatus getTable() {
            return table;
        }

        public Exception getTableFailure() {
            return tableFailure;
        }

        public List<StateStoreUpdateFailure> getStateStoreUpdateFailures() {
            return stateStoreUpdateFailures;
        }

        public List<FileFailure> getFileFailures() {
            return fileFailures;
        }

        public Stream<Exception> streamFailures() {
            return Stream.of(
                    Stream.ofNullable(tableFailure),
                    stateStoreUpdateFailures.stream().map(StateStoreUpdateFailure::getCause),
                    fileFailures.stream().map(FileFailure::getCause))
                    .flatMap(s -> s);
        }
    }

    public static class FileFailure {
        private final String filename;
        private final Exception cause;

        public FileFailure(String filename, Exception cause) {
            this.filename = filename;
            this.cause = cause;
        }

        public String getFilename() {
            return filename;
        }

        public Exception getCause() {
            return cause;
        }
    }

    public static class StateStoreUpdateFailure {
        private final List<String> filenames;
        private final Exception cause;

        public StateStoreUpdateFailure(List<String> filenames, Exception cause) {
            this.filenames = filenames;
            this.cause = cause;
        }

        public List<String> getFilenames() {
            return filenames;
        }

        public Exception getCause() {
            return cause;
        }
    }
}
