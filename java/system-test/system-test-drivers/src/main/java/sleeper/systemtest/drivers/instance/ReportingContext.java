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

package sleeper.systemtest.drivers.instance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

public class ReportingContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReportingContext.class);

    private final Path outputDirectory;
    private Instant recordingStartTime = Instant.now();

    public ReportingContext(SystemTestParameters parameters) {
        this(parameters.getOutputDirectory());
    }

    public ReportingContext(Path outputDirectory) {
        this.outputDirectory = outputDirectory;
    }

    public void startRecording() {
        recordingStartTime = Instant.now();
        LOGGER.info("Ingest recording window started at {}", recordingStartTime);
    }

    public Instant getRecordingStartTime() {
        return recordingStartTime;
    }

    public void print(TestContext testContext, SystemTestReport report) {
        try (ReportHandle handle = openReport(testContext)) {
            report.print(handle.getPrintStream(), recordingStartTime);
        }
    }

    private ReportHandle openReport(TestContext testContext) {
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
                outputStream = Files.newOutputStream(file, CREATE, APPEND);
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
