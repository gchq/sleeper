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
package sleeper.status.report;

import sleeper.ingest.task.IngestTaskStatusStore;
import sleeper.status.report.ingest.task.IngestTaskQuery;
import sleeper.status.report.ingest.task.IngestTaskStatusReportArguments;
import sleeper.status.report.ingest.task.IngestTaskStatusReporter;

public class IngestTaskStatusReport {
    private final IngestTaskStatusStore statusStore;

    private final IngestTaskStatusReporter reporter;
    private final IngestTaskQuery query;

    public IngestTaskStatusReport(
            IngestTaskStatusStore statusStore,
            IngestTaskStatusReporter reporter,
            IngestTaskQuery query) {
        this.statusStore = statusStore;
        this.reporter = reporter;
        this.query = query;
    }

    public void run() {
        reporter.report(query, query.run(statusStore));
    }

    public static void main(String[] args) {
        IngestTaskStatusReportArguments arguments;
        try {
            arguments = IngestTaskStatusReportArguments.fromArgs(args);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            IngestTaskStatusReportArguments.printUsage(System.err);
            System.exit(1);
            return;
        }

        IngestTaskStatusStore statusStore = IngestTaskStatusStore.none();
        new IngestTaskStatusReport(statusStore, arguments.getReporter(), arguments.getQuery()).run();
    }
}
