/*
 * Copyright 2022 Crown Copyright
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

package sleeper.status.report.compaction.job;

import sleeper.console.ConsoleInput;
import sleeper.console.ConsoleOutput;
import sleeper.status.report.compaction.job.query.AllCompactionJobQuery;
import sleeper.status.report.compaction.job.query.DetailedCompactionJobQuery;
import sleeper.status.report.compaction.job.query.UnfinishedCompactionJobQuery;

public class CompactionJobQueryPrompt {
    private final String tableName;
    private final ConsoleInput in;
    private final ConsoleOutput out;

    private CompactionJobQueryPrompt(String tableName, ConsoleInput in, ConsoleOutput out) {
        this.tableName = tableName;
        this.in = in;
        this.out = out;
    }

    public static CompactionJobQuery from(String tableName, ConsoleInput input, ConsoleOutput output) {
        return new CompactionJobQueryPrompt(tableName, input, output).promptForQuery();
    }

    private CompactionJobQuery promptForQuery() {
        String type = in.promptLine("All (a), Detailed (d), range (r), or unfinished (u) query? ");
        if (type.equalsIgnoreCase("a")) {
            return new AllCompactionJobQuery(tableName);
        } else if (type.equalsIgnoreCase("u")) {
            return new UnfinishedCompactionJobQuery(tableName);
        } else if (type.equalsIgnoreCase("d")) {
            String jobIds = in.promptLine("Enter jobId to get detailed information about: ");
            return DetailedCompactionJobQuery.fromParameters(jobIds);
        }
        return null;
    }
}
