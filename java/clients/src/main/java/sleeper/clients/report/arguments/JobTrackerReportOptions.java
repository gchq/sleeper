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
import sleeper.core.util.cli.CommandOption.NumArgs;

import java.util.List;

public class JobTrackerReportOptions {

    private static final CommandOption ALL = CommandOption.withLongName("all").shortName('a').build();
    private static final CommandOption DETAILED = CommandOption.withLongName("detailed").shortName('d').numArgs(NumArgs.ONE).build();
    private static final CommandOption RANGE = CommandOption.withLongName("range").shortName('r').build();
    private static final CommandOption UNFINISHED = CommandOption.withLongName("unfinished").shortName('u').build();
    private static final CommandOption START_TIME = CommandOption.withLongName("start-time").numArgs(NumArgs.ONE).build();
    private static final CommandOption END_TIME = CommandOption.withLongName("end-time").numArgs(NumArgs.ONE).build();
    // Note that compaction jobs are never rejected, so this only applies to ingest.
    private static final CommandOption REJECTED = CommandOption.withLongName("rejected").shortName('n').build();

    public static List<CommandOption> forIngest() {
        return List.of(ALL, DETAILED, END_TIME, RANGE, REJECTED, ReportTypeArgument.option(), START_TIME, UNFINISHED);
    }

}
