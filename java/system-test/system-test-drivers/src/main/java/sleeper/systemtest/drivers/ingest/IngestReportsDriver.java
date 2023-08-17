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
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.sqs.AmazonSQS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.status.report.IngestJobStatusReport;
import sleeper.clients.status.report.IngestTaskStatusReport;
import sleeper.clients.status.report.ingest.job.PersistentEMRStepCount;
import sleeper.clients.status.report.ingest.job.StandardIngestJobStatusReporter;
import sleeper.clients.status.report.ingest.task.IngestTaskQuery;
import sleeper.clients.status.report.ingest.task.StandardIngestTaskStatusReporter;
import sleeper.clients.status.report.job.query.JobQuery;
import sleeper.clients.status.report.job.query.RangeJobsQuery;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.IngestJobStatusStoreFactory;
import sleeper.ingest.status.store.task.IngestTaskStatusStoreFactory;
import sleeper.ingest.task.IngestTaskStatusStore;
import sleeper.job.common.QueueMessageCount;
import sleeper.systemtest.drivers.instance.ReportingContext;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.instance.SystemTestParameters;
import sleeper.systemtest.drivers.util.TestContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

public class IngestReportsDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestReportsDriver.class);

    private final IngestJobStatusStore ingestJobStatusStore;
    private final IngestTaskStatusStore ingestTaskStatusStore;
    private final SleeperInstanceContext instance;
    private final SystemTestParameters parameters;
    private final QueueMessageCount.Client queueClient;
    private final AmazonElasticMapReduce emrClient;
    private final ReportingContext reportingContext;

    public IngestReportsDriver(AmazonDynamoDB dynamoDB, AmazonSQS sqs, AmazonElasticMapReduce emrClient,
                               SleeperInstanceContext instance, SystemTestParameters parameters,
                               ReportingContext reportingContext) {
        InstanceProperties properties = instance.getInstanceProperties();
        this.ingestJobStatusStore = IngestJobStatusStoreFactory.getStatusStore(dynamoDB, properties);
        this.ingestTaskStatusStore = IngestTaskStatusStoreFactory.getStatusStore(dynamoDB, properties);
        this.instance = instance;
        this.parameters = parameters;
        this.queueClient = QueueMessageCount.withSqsClient(sqs);
        this.emrClient = emrClient;
        this.reportingContext = reportingContext;
    }

    public void printTasksAndJobs(TestContext testContext) {
        try (ReportHandle handle = openReport(testContext)) {
            PrintStream out = handle.getPrintStream();
            new IngestTaskStatusReport(ingestTaskStatusStore,
                    new StandardIngestTaskStatusReporter(out),
                    IngestTaskQuery.forPeriod(reportingContext.getRecordingStartTime(), Instant.MAX))
                    .run();
            new IngestJobStatusReport(ingestJobStatusStore, JobQuery.Type.RANGE,
                    new RangeJobsQuery(instance.getTableName(), reportingContext.getRecordingStartTime(), Instant.MAX),
                    new StandardIngestJobStatusReporter(out), queueClient, instance.getInstanceProperties(),
                    PersistentEMRStepCount.byStatus(instance.getInstanceProperties(), emrClient))
                    .run();
        }
    }

    private ReportHandle openReport(TestContext testContext) {
        Path outputDirectory = parameters.getOutputDirectory();
        if (outputDirectory != null) {
            return new FileWriter(outputDirectory.resolve(
                    testContext.getTestClassAndMethod() + ".report.log"));
        } else {
            return new LogWriter();
        }
    }

    interface ReportHandle extends AutoCloseable {
        PrintStream getPrintStream();

        void close();
    }

    private static class FileWriter implements ReportHandle {
        private final OutputStream outputStream;
        private final PrintStream printStream;

        FileWriter(Path file) {
            try {
                outputStream = Files.newOutputStream(file);
                printStream = new PrintStream(outputStream, false, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public PrintStream getPrintStream() {
            return printStream;
        }

        @Override
        public void close() {
            try {
                printStream.flush();
                outputStream.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static class LogWriter implements ReportHandle {
        private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        private final PrintStream printStream = new PrintStream(outputStream, false, StandardCharsets.UTF_8);

        @Override
        public PrintStream getPrintStream() {
            return printStream;
        }

        @Override
        public void close() {
            printStream.close();
            LOGGER.info("Reports:\n{}", outputStream.toString(StandardCharsets.UTF_8));
        }
    }
}
