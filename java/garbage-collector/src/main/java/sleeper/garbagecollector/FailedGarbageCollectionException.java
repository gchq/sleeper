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
                tableFailures.stream().map(TableFailures::table).collect(toUnmodifiableList());
    }

    private static Exception getCause(List<TableFailures> tableFailures) {
        return tableFailures.stream()
                .flatMap(table -> table.streamFailures())
                .findFirst().orElse(null);
    }

    public List<TableFailures> getTableFailures() {
        return tableFailures;
    }

    public record TableFailures(TableStatus table, Exception tableFailure, List<FileFailure> fileFailures, List<StateStoreUpdateFailure> stateStoreUpdateFailures) {

        public Stream<Exception> streamFailures() {
            return Stream.of(
                    Stream.ofNullable(tableFailure),
                    stateStoreUpdateFailures.stream().map(StateStoreUpdateFailure::cause),
                    fileFailures.stream().map(FileFailure::cause))
                    .flatMap(s -> s);
        }
    }

    public record FileFailure(String filename, Exception cause) {
    }

    public record StateStoreUpdateFailure(List<String> filenames, Exception cause) {
    }
}
