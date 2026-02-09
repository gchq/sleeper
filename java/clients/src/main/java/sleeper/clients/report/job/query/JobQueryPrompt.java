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
import java.util.Map;

/**
 * Prompts the user on the command line to create a query to generate a report from a job tracker.
 */
public class JobQueryPrompt {

    private JobQueryPrompt() {
    }

    /**
     * Creates a query by prompting the user. This can be used to generate a report from a job tracker.
     *
     * @param  table        the Sleeper table to query
     * @param  clock        a clock to get the current time (can be fixed for tests)
     * @param  in           the console to prompt the user
     * @param  extraQueries specific queries to allow for this prompt
     * @return              the query
     */
    public static JobQuery from(TableStatus table, Clock clock, ConsoleInput in, Map<String, JobQuery> extraQueries) {
        String type = in.promptLine("All (a), Detailed (d), range (r), or unfinished (u) query? ");
        if ("".equals(type)) {
            return null;
        } else if ("a".equalsIgnoreCase(type)) {
            return new AllJobsQuery(table);
        } else if ("u".equalsIgnoreCase(type)) {
            return new UnfinishedJobsQuery(table);
        } else if ("d".equalsIgnoreCase(type)) {
            String jobIds = in.promptLine("Enter jobId to get detailed information about: ");
            return DetailedJobsQuery.fromParameters(jobIds);
        } else if ("r".equalsIgnoreCase(type)) {
            return RangeJobsQuery.prompt(table, in, clock);
        } else if (extraQueries.containsKey(type)) {
            return extraQueries.get(type);
        } else {
            return from(table, clock, in, extraQueries);
        }
    }
}
