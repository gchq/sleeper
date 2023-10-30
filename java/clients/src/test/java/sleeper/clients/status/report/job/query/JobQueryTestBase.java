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

package sleeper.clients.status.report.job.query;

import sleeper.clients.status.report.job.query.JobQuery.Type;
import sleeper.clients.testutil.TestConsoleInput;
import sleeper.clients.testutil.ToStringPrintStream;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.CompactionJobStatusTestData;
import sleeper.compaction.job.CompactionJobTestDataHelper;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.table.TableId;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class JobQueryTestBase {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTableProperties();
    protected static final String TABLE_NAME = "test-table";
    protected final TableId tableId = tableProperties.getId();
    protected final CompactionJobStatusStore statusStore = mock(CompactionJobStatusStore.class);
    private final CompactionJobTestDataHelper dataHelper = CompactionJobTestDataHelper.forTable(instanceProperties, tableProperties);
    protected final CompactionJob exampleJob1 = dataHelper.singleFileCompaction();
    protected final CompactionJob exampleJob2 = dataHelper.singleFileCompaction();
    protected final CompactionJobStatus exampleStatus1 = CompactionJobStatusTestData.jobCreated(
            exampleJob1, Instant.parse("2022-09-22T13:33:12.001Z"));
    protected final CompactionJobStatus exampleStatus2 = CompactionJobStatusTestData.jobCreated(
            exampleJob2, Instant.parse("2022-09-22T13:53:12.001Z"));
    protected final List<CompactionJobStatus> exampleStatusList = Arrays.asList(exampleStatus2, exampleStatus1);
    protected final ToStringPrintStream out = new ToStringPrintStream();
    protected final TestConsoleInput in = new TestConsoleInput(out.consoleOut());

    protected List<CompactionJobStatus> queryStatuses(Type queryType) {
        return queryStatusesWithParams(queryType, null);
    }

    protected List<CompactionJobStatus> queryStatusesWithParams(Type queryType, String queryParameters) {
        return queryStatuses(queryType, queryParameters, Clock.systemUTC());
    }

    protected List<CompactionJobStatus> queryStatusesAtTime(Type queryType, Instant time) {
        return queryStatuses(queryType, null,
                Clock.fixed(time, ZoneId.of("UTC")));
    }

    protected JobQuery queryFrom(Type queryType) {
        return queryFrom(queryType, null, Clock.systemUTC());
    }

    private List<CompactionJobStatus> queryStatuses(Type queryType, String queryParameters, Clock clock) {
        return queryFrom(queryType, queryParameters, clock).run(statusStore);
    }

    private JobQuery queryFrom(Type queryType, String queryParameters, Clock clock) {
        return JobQuery.fromParametersOrPrompt(tableProperties.getId(), queryType, queryParameters, clock, in.consoleIn());
    }

    private TableProperties createTableProperties() {
        TableProperties properties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
        properties.set(TableProperty.TABLE_NAME, TABLE_NAME);
        return properties;
    }
}
