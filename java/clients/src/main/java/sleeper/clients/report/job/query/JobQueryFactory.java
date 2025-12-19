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

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Creates a job query of a certain type from arguments passed on the command line, and prompts if necessary. Used with
 * {@link JobQueryType}.
 */
public interface JobQueryFactory {

    /**
     * Creates the query.
     *
     * @param  reader          the reader for job queries
     * @param  queryParameters the parameters
     * @return                 the query
     */
    JobQuery createQuery(JobQueryReader reader, String queryParameters);

    public static JobQueryFactory fromPrompts() {
        return (reader, parameters) -> reader.createQueryFromPrompts();
    }

    public static JobQueryFactory fromTableStatus(Function<TableStatus, JobQuery> constructor) {
        return (reader, parameters) -> constructor.apply(reader.getTableStatus());
    }

    public static JobQueryFactory from(Supplier<JobQuery> supplier) {
        return (reader, parameters) -> supplier.get();
    }

    public static JobQueryFactory fromParameters(Function<String, JobQuery> fromParameters, Function<ConsoleInput, JobQuery> fromPrompts) {
        return (reader, parameters) -> {
            if (parameters == null) {
                return fromPrompts.apply(reader.getConsole());
            } else {
                return fromParameters.apply(parameters);
            }
        };
    }

}
