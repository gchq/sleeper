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
package sleeper.clients.report.job.query;

import sleeper.clients.util.console.ConsoleInput;
import sleeper.core.table.TableStatus;

import java.time.Clock;
import java.util.List;
import java.util.Optional;

/**
 * Creates queries of jobs of available types, for inclusion in a report.
 */
public class JobQueryReader {

    private final TableStatus tableStatus;
    private final List<JobQueryType> types;
    private final Clock clock;
    private final ConsoleInput console;

    public JobQueryReader(TableStatus tableStatus, List<JobQueryType> types) {
        this(tableStatus, types, Clock.systemUTC(), new ConsoleInput(System.console()));
    }

    public JobQueryReader(TableStatus tableStatus, List<JobQueryType> types, Clock clock, ConsoleInput console) {
        this.tableStatus = tableStatus;
        this.types = types;
        this.clock = clock;
        this.console = console;
    }

    public JobQuery createQueryFromPrompts() {
        Optional<JobQueryType> typeOpt = Optional.empty();
        while (typeOpt.isEmpty()) {
            typeOpt = typeByShortName(console.promptLine(queryTypePrompt()));
        }
        JobQueryType type = typeOpt.get();
        return type.getFactory().createQuery(this, null);
    }

    public TableStatus getTableStatus() {
        return tableStatus;
    }

    public ConsoleInput getConsole() {
        return console;
    }

    public Clock getClock() {
        return clock;
    }

    private String queryTypePrompt() {
        // TODO
        return "All (a), Detailed (d), range (r), or unfinished (u) query? ";
    }

    private Optional<JobQueryType> typeByShortName(String shortName) {
        return types.stream()
                .filter(type -> type.getShortName().equals(shortName))
                .findFirst();
    }

}
