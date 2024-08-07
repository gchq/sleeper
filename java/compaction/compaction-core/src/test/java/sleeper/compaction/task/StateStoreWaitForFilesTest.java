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
package sleeper.compaction.task;

import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.core.util.ExponentialBackoffWithJitter.Waiter;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithSinglePartition;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.noJitter;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.recordWaits;

public class StateStoreWaitForFilesTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = schemaWithKey("key");
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final StateStore stateStore = inMemoryStateStoreWithSinglePartition(schema);
    private final FileReferenceFactory factory = FileReferenceFactory.from(stateStore);
    private final List<Duration> foundWaits = new ArrayList<>();
    private Waiter waiter = recordWaits(foundWaits);

    @Test
    void shouldSkipWaitIfFilesAreAlreadyAssignedToJob() throws Exception {
        // Given
        FileReference file = factory.rootFile("test.parquet", 123L);
        stateStore.addFile(file);
        CompactionJob job = jobForFileAtRoot(file);
        stateStore.assignJobIds(List.of(assignJobOnPartitionToFiles(job.getId(), job.getPartitionId(), job.getInputFiles())));

        // When
        waitForFiles(job);

        // Then
        assertThat(foundWaits).isEmpty();
    }

    @Test
    void shouldRetryThenCheckFilesAreAssignedToJob() throws Exception {
        // Given
        FileReference file = factory.rootFile("test.parquet", 123L);
        stateStore.addFile(file);
        CompactionJob job = jobForFileAtRoot(file);
        actionOnWait(() -> {
            stateStore.assignJobIds(List.of(assignJobOnPartitionToFiles(job.getId(), job.getPartitionId(), job.getInputFiles())));
        });

        // When
        waitForFiles(job);

        // Then
        assertThat(foundWaits).hasSize(1);
    }

    @Test
    void shouldTimeOutIfFilesAreNeverAssignedToJob() throws Exception {
        // Given
        FileReference file = factory.rootFile("test.parquet", 123L);
        stateStore.addFile(file);
        CompactionJob job = jobForFileAtRoot(file);

        // When / Then
        assertThatThrownBy(() -> waitForFiles(job))
                .isInstanceOf(TimedOutWaitingForFileAssignmentsException.class);
        assertThat(foundWaits).hasSize(1);
    }

    private CompactionJob jobForFileAtRoot(FileReference... files) {
        return new CompactionJobFactory(instanceProperties, tableProperties).createCompactionJob(List.of(files), "root");
    }

    private void waitForFiles(CompactionJob job) throws Exception {
        new StateStoreWaitForFiles(2,
                new ExponentialBackoffWithJitter(
                        StateStoreWaitForFiles.JOB_ASSIGNMENT_WAIT_RANGE,
                        noJitter(), waiter),
                Map.of(tableProperties.get(TABLE_ID), stateStore)::get)
                .wait(job);
    }

    protected void actionOnWait(WaitAction action) throws Exception {
        Waiter wrapWaiter = waiter;
        waiter = millis -> {
            try {
                action.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            wrapWaiter.waitForMillis(millis);
        };
    }

    protected interface WaitAction {
        void run() throws Exception;
    }
}
