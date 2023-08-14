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

package sleeper.systemtest.drivers.ingest;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.status.report.IngestTaskStatusReport;
import sleeper.clients.status.report.ingest.task.IngestTaskQuery;
import sleeper.clients.status.report.ingest.task.StandardIngestTaskStatusReporter;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.IngestJobStatusStoreFactory;
import sleeper.ingest.status.store.task.IngestTaskStatusStoreFactory;
import sleeper.ingest.task.IngestTaskStatusStore;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

public class IngestReportsDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestStatusStoreDriver.class);

    private final IngestJobStatusStore ingestJobStatusStore;
    private final IngestTaskStatusStore ingestTaskStatusStore;
    private final SleeperInstanceContext instance;

    public IngestReportsDriver(AmazonDynamoDB dynamoDB, SleeperInstanceContext instance) {
        InstanceProperties properties = instance.getInstanceProperties();
        this.ingestJobStatusStore = IngestJobStatusStoreFactory.getStatusStore(dynamoDB, properties);
        this.ingestTaskStatusStore = IngestTaskStatusStoreFactory.getStatusStore(dynamoDB, properties);
        this.instance = instance;
    }

    public void printReports() {
        printReport(out -> new IngestTaskStatusReport(ingestTaskStatusStore,
                new StandardIngestTaskStatusReporter(out),
                IngestTaskQuery.ALL).run());
    }

    private void printReport(Consumer<PrintStream> writer) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(os, false, StandardCharsets.UTF_8);
        writer.accept(printStream);
        LOGGER.info(os.toString(StandardCharsets.UTF_8));
    }
}
