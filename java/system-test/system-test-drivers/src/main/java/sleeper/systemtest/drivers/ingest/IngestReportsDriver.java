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

public class IngestReportsDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestStatusStoreDriver.class);

    private final IngestJobStatusStore ingestJobStatusStore;
    private final IngestTaskStatusStore ingestTaskStatusStore;
    private final SleeperInstanceContext instance;
    private final SystemTestParameters parameters;

    public IngestReportsDriver(AmazonDynamoDB dynamoDB, SleeperInstanceContext instance, SystemTestParameters parameters) {
        InstanceProperties properties = instance.getInstanceProperties();
        this.ingestJobStatusStore = IngestJobStatusStoreFactory.getStatusStore(dynamoDB, properties);
        this.ingestTaskStatusStore = IngestTaskStatusStoreFactory.getStatusStore(dynamoDB, properties);
        this.instance = instance;
        this.parameters = parameters;
    }

    public void printReports(TestContext testContext) {
        try (ReportHandle handle = openReport(testContext)) {
            PrintStream out = handle.getPrintStream();
            new IngestTaskStatusReport(ingestTaskStatusStore,
                    new StandardIngestTaskStatusReporter(out),
                    IngestTaskQuery.ALL).run();
        }
    }

    private ReportHandle openReport(TestContext testContext) {
        Path outputDirectory = parameters.getOutputDirectory();
        if (outputDirectory != null) {
            return new FileWriter(outputDirectory.resolve(testFileName(testContext)));
        } else {
            return new LogWriter();
        }
    }

    private static String testFileName(TestContext context) {
        return context.getTestClass().orElseThrow().getSimpleName()
                + "." + context.getTestMethod().orElseThrow().getName();
    }

    interface ReportHandle extends AutoCloseable {
        PrintStream getPrintStream();

        void close();
    }

    private static class FileWriter implements ReportHandle {
        private final OutputStream outputStream;
        private final PrintStream printStream;

        public FileWriter(Path file) {
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
