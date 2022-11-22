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

package sleeper.status.report.ingest.job;

import sleeper.status.report.table.TableField;
import sleeper.status.report.table.TableWriterFactory;

import java.io.PrintStream;

public class IngestJobStatusReport {
    private static final TableWriterFactory.Builder TABLE_FACTORY_BUILDER = TableWriterFactory.builder();

    private static final TableField STATE = TABLE_FACTORY_BUILDER.addField("STATE");
    private static final TableField JOB_ID = TABLE_FACTORY_BUILDER.addField("JOB_ID");
    private static final TableField TASK_ID = TABLE_FACTORY_BUILDER.addField("TASK_ID");
    private static final TableField START_TIME = TABLE_FACTORY_BUILDER.addField("START_TIME");
    private static final TableField FINISH_TIME = TABLE_FACTORY_BUILDER.addField("FINISH_TIME");
    private static final TableField DURATION = TABLE_FACTORY_BUILDER.fieldBuilder("DURATION (s)").alignRight().build();
    private static final TableField TOTAL_FILES = TABLE_FACTORY_BUILDER.addField("TOTAL_FILES");
    private static final TableField LINES_READ = TABLE_FACTORY_BUILDER.fieldBuilder("LINES_READ").alignRight().build();
    private static final TableField LINES_WRITTEN = TABLE_FACTORY_BUILDER.fieldBuilder("LINES_WRITTEN").alignRight().build();
    private static final TableField READ_RATE = TABLE_FACTORY_BUILDER.fieldBuilder("READ_RATE (s)").alignRight().build();
    private static final TableField WRITE_RATE = TABLE_FACTORY_BUILDER.fieldBuilder("WRITE_RATE (s)").alignRight().build();
    private static final TableWriterFactory TABLE_FACTORY = TABLE_FACTORY_BUILDER.build();

    private final PrintStream out;

    public IngestJobStatusReport(PrintStream out) {
        this.out = out;
    }

    public void run(IngestJobQuery query) {
        out.println();
        out.println("Ingest Job Status Report");
        out.println("------------------------");
        out.printf("Total jobs pending in queue: %s%n", 0);
        out.printf("Total jobs in progress: %s%n", 0);
        out.printf("Total jobs finished: %s%n", 0);
        TABLE_FACTORY.tableBuilder()
                .build().write(out);
    }
}
