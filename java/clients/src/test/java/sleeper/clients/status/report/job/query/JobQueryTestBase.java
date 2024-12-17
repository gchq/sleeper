/*
 * Copyright 2022-2024 Crown Copyright
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
import sleeper.clients.testutil.ToStringConsoleOutput;
import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobStatusTestData;
import sleeper.compaction.core.job.CompactionJobTestDataHelper;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.tracker.compaction.job.CompactionJobStatusStore;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class JobQueryTestBase {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTableProperties();
    protected static final String TABLE_NAME = "test-table";
    protected final String tableId = tableProperties.get(TABLE_ID);
    protected final CompactionJobStatusStore statusStore = mock(CompactionJobStatusStore.class);
    private final CompactionJobTestDataHelper dataHelper = CompactionJobTestDataHelper.forTable(instanceProperties, tableProperties);
    protected final CompactionJob exampleJob1 = dataHelper.singleFileCompaction();
    protected final CompactionJob exampleJob2 = dataHelper.singleFileCompaction();
    protected final CompactionJobStatus exampleStatus1 = CompactionJobStatusTestData.jobCreated(
            exampleJob1, Instant.parse("2022-09-22T13:33:12.001Z"));
    protected final CompactionJobStatus exampleStatus2 = CompactionJobStatusTestData.jobCreated(
            exampleJob2, Instant.parse("2022-09-22T13:53:12.001Z"));
    protected final List<CompactionJobStatus> exampleStatusList = Arrays.asList(exampleStatus2, exampleStatus1);
    protected final ToStringConsoleOutput out = new ToStringConsoleOutput();
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
        return JobQuery.fromParametersOrPrompt(tableProperties.getStatus(), queryType, queryParameters, clock, in.consoleIn());
    }

    private TableProperties createTableProperties() {
        TableProperties properties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
        properties.set(TableProperty.TABLE_NAME, TABLE_NAME);
        return properties;
    }
}
