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

package sleeper.clients.status.report.job.query;

import sleeper.clients.util.console.ConsoleInput;

import java.time.Clock;

public class JobQueryPrompt {

    private JobQueryPrompt() {
    }

    public static JobQuery from(String tableName, Clock clock, ConsoleInput in) {
        String type = in.promptLine("All (a), Detailed (d), range (r), or unfinished (u) query? ");
        if ("".equals(type)) {
            return null;
        } else if (type.equalsIgnoreCase("a")) {
            return new AllJobsQuery(tableName);
        } else if (type.equalsIgnoreCase("u")) {
            return new UnfinishedJobsQuery(tableName);
        } else if (type.equalsIgnoreCase("d")) {
            String jobIds = in.promptLine("Enter jobId to get detailed information about: ");
            return DetailedJobsQuery.fromParameters(jobIds);
        } else if (type.equalsIgnoreCase("r")) {
            return RangeJobsQuery.prompt(tableName, in, clock);
        } else {
            return from(tableName, clock, in);
        }
    }
}
