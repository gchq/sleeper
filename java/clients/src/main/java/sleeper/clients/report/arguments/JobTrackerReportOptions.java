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
package sleeper.clients.report.arguments;

import sleeper.core.util.cli.CommandOption;

import java.util.List;

public class JobTrackerReportOptions {

    private static final CommandOption ALL = CommandOption.shortFlag('a', "all");
    private static final CommandOption DETAILED = CommandOption.shortOption('d', "detailed");
    private static final CommandOption RANGE = CommandOption.shortFlag('r', "range");
    private static final CommandOption UNFINISHED = CommandOption.shortFlag('u', "unfinished");
    private static final CommandOption START_TIME = CommandOption.longOption("start-time");
    private static final CommandOption END_TIME = CommandOption.longOption("end-time");
    // Note that compaction jobs are never rejected, so this only applies to ingest.
    private static final CommandOption REJECTED = CommandOption.shortFlag('n', "rejected");

    public static List<CommandOption> forIngest() {
        return List.of(ALL, DETAILED, END_TIME, RANGE, REJECTED, ReportTypeArgument.option(), START_TIME, UNFINISHED);
    }

}
