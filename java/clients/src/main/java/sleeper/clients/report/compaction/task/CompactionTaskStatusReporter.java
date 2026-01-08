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
package sleeper.clients.report.compaction.task;

import sleeper.core.tracker.compaction.task.CompactionTaskStatus;

import java.io.PrintStream;
import java.util.List;
import java.util.Locale;

/**
 * Writes reports on the status of compaction tasks. The format and output destination can vary based on the
 * implementation.
 */
@FunctionalInterface
public interface CompactionTaskStatusReporter {

    /**
     * Writes a report on the status of compaction tasks.
     *
     * @param query the query for the report
     * @param tasks the data retrieved from the task tracker
     */
    void report(CompactionTaskQuery query, List<CompactionTaskStatus> tasks);

    /**
     * Creates a reporter of the given type as specified on the command line.
     *
     * @param  type                     the reporter type command line argument
     * @param  out                      the output to write the report to
     * @return                          the reporter
     * @throws IllegalArgumentException if the reporter type is unrecognised
     */
    static CompactionTaskStatusReporter from(String type, PrintStream out) {
        switch (type.toLowerCase(Locale.ROOT)) {
            case "standard":
                return new StandardCompactionTaskStatusReporter(out);
            case "json":
                return new JsonCompactionTaskStatusReporter(out);
            default:
                throw new IllegalArgumentException("Unrecognised reporter type: " + type);
        }
    }
}
