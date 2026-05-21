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
package sleeper.clients.report.tables;

import sleeper.core.table.TableStatus;

import java.util.stream.Stream;

/**
 * A reporter to list the tables in a Sleeper instance to a user.
 */
public interface ListTablesReporter {

    /**
     * Writes a report on the tables.
     *
     * @param tables stream of Sleeper tables
     */
    void report(Stream<TableStatus> tables);
}
