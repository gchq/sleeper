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
package sleeper.compaction.job.testutils;

import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobFactory;
import sleeper.compaction.job.status.CompactionJobCreatedStatus;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.statestore.FileInfoFactory;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.CompactionJobStatusTestData.defaultUpdateTime;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobCreated;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobStatusFrom;
import static sleeper.compaction.job.CompactionJobStatusTestData.startedCompactionStatus;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.forJob;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.forJobOnTask;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.records;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

class CompactionJobStatusStoreInMemoryTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = schemaWithKey("key");
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key", new StringType()));
    private final CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, tableProperties);
    private final FileInfoFactory fileFactory = new FileInfoFactory(schema, new PartitionsBuilder(schema).singlePartition("root").buildList());
    private final CompactionJobStatusStoreInMemory store = new CompactionJobStatusStoreInMemory();

    @Test
    void shouldGetCreatedJob() {
        Instant storeTime = fixStoreTime(Instant.parse("2023-03-29T12:27:42Z"));
        CompactionJob job = addCreatedJob();

        assertThat(store.getAllJobs(tableProperties.get(TABLE_NAME)))
                .containsExactly(jobCreated(job, storeTime));
    }

    @Test
    void shouldGetStartedJob() {
        Instant createdTime = fixStoreTime(Instant.parse("2023-03-29T12:27:42Z"));
        CompactionJob job = addCreatedJob();
        Instant startedTime = Instant.parse("2023-03-29T12:27:43Z");
        fixStoreTime(defaultUpdateTime(startedTime));
        store.jobStarted(job, startedTime, "test-task");

        assertThat(store.getAllJobs(tableProperties.get(TABLE_NAME)))
                .containsExactly(jobStatusFrom(records().fromUpdates(
                        forJob(job.getId(), CompactionJobCreatedStatus.from(job, createdTime)),
                        forJobOnTask(job.getId(), "test-task", startedCompactionStatus(startedTime)))));
    }

    private Instant fixStoreTime(Instant now) {
        store.fixTime(now);
        return now;
    }

    private CompactionJob addCreatedJob() {
        CompactionJob job = createCompactionJob();
        store.jobCreated(job);
        return job;
    }

    private CompactionJob createCompactionJob() {
        return jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile(100, "a", "z")),
                "root");
    }
}
