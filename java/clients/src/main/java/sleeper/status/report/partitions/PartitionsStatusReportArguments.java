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

package sleeper.status.report.partitions;

import java.io.PrintStream;

public class PartitionsStatusReportArguments {
    private final String instanceId;
    private final String tableName;
    private final PartitionsStatusReporter reporter;

    private PartitionsStatusReportArguments(String instanceId,
                                            String tableName,
                                            PartitionsStatusReporter reporter) {
        this.instanceId = instanceId;
        this.tableName = tableName;
        this.reporter = reporter;
    }

    public static void printUsage(PrintStream out) {
        out.println("Usage: <instance id> <table name>");
    }

    public static PartitionsStatusReportArguments fromArgs(String... args) {
        if (args.length != 2) {
            throw new IllegalArgumentException("Wrong number of arguments");
        }
        PartitionsStatusReporter reporter = new PartitionsStatusReporter(System.out);
        return new PartitionsStatusReportArguments(args[0], args[1], reporter);
    }

    public String getInstanceId() {
        return instanceId;
    }

    public String getTableName() {
        return tableName;
    }

    public PartitionsStatusReporter getReporter() {
        return reporter;
    }
}
