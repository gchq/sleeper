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

package sleeper.clients.report.filestatus;

import com.google.common.io.CharStreams;
import org.junit.jupiter.api.BeforeEach;

import sleeper.clients.report.FilesStatusReport;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;

public class FilesStatusReportTestBase {
    protected final Schema schema = Schema.builder().rowKeyFields(new Field("key1", new StringType())).build();
    protected Instant lastStateStoreUpdate = Instant.parse("2022-08-22T14:20:00.001Z");

    protected final InstanceProperties instanceProperties = createTestInstanceProperties();
    protected final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    protected final StateStore stateStore = InMemoryTransactionLogStateStore.create(tableProperties, new InMemoryTransactionLogs());
    protected final Map<String, CompactionJobStatus> jobIdToStatus = new HashMap<>();

    @BeforeEach
    void setUpBase() {
        stateStore.fixFileUpdateTime(lastStateStoreUpdate);
    }

    protected void setTime(Instant timeNow) {
        lastStateStoreUpdate = timeNow;
        stateStore.fixFileUpdateTime(timeNow);
    }

    protected void moveTimeForward(Duration duration) {
        lastStateStoreUpdate = lastStateStoreUpdate.plus(duration);
        stateStore.fixFileUpdateTime(lastStateStoreUpdate);
    }

    protected void setJobTrackerData(CompactionJobStatus... jobs) {
        for (CompactionJobStatus job : jobs) {
            jobIdToStatus.put(job.getJobId(), job);
        }
    }

    protected static String example(String path) throws IOException {
        try (Reader reader = new InputStreamReader(FilesStatusReportTestBase.class.getClassLoader().getResourceAsStream(path))) {
            return CharStreams.toString(reader);
        }
    }

    protected String verboseReportString(Function<PrintStream, FileStatusReporter> getReporter) {
        return verboseReportStringWithMaxFilesWithNoReferences(getReporter, 100);
    }

    protected String standardReportString(FileStatusDetailLevel detailLevel) {
        return reportStringWithMaxFilesWithNoReferences(StandardFileStatusReporter::new, detailLevel, 100);
    }

    protected String verboseReportStringWithMaxFilesWithNoReferences(Function<PrintStream, FileStatusReporter> getReporter, int maxFilesWithNoReferences) {
        return reportStringWithMaxFilesWithNoReferences(getReporter, FileStatusDetailLevel.FULL, maxFilesWithNoReferences);
    }

    protected String reportStringWithMaxFilesWithNoReferences(Function<PrintStream, FileStatusReporter> getReporter, FileStatusDetailLevel detailLevel, int maxFilesWithNoReferences) {
        try {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            FileStatusReporter reporter = getReporter.apply(
                    new PrintStream(os, false, StandardCharsets.UTF_8.displayName()));
            new FilesStatusReport(stateStore, maxFilesWithNoReferences, detailLevel, reporter, id -> Optional.ofNullable(jobIdToStatus.get(id))).run();
            return os.toString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
