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
package sleeper.compaction.status.job;

import org.junit.Before;
import org.junit.Test;
import sleeper.compaction.job.CompactionFactory;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.schema.Schema;
import sleeper.statestore.FileInfoFactory;

import java.time.Instant;
import java.util.Collections;

import static sleeper.compaction.status.job.CompactionStatusStoreTestUtils.createInstanceProperties;
import static sleeper.compaction.status.job.CompactionStatusStoreTestUtils.createSchema;
import static sleeper.compaction.status.job.CompactionStatusStoreTestUtils.createTableProperties;

public class DynamoDBCompactionJobStatusStoreIT extends DynamoDBTestBase {

    private final InstanceProperties instanceProperties = createInstanceProperties();

    private final Schema schema = createSchema();
    private final TableProperties tableProperties = createTableProperties(schema, instanceProperties);
    private final CompactionJobStatusStore store = DynamoDBCompactionJobStatusStore.from(dynamoDBClient, instanceProperties);

    @Before
    public void setUp() {
        DynamoDBCompactionJobStatusStoreCreator.create(instanceProperties, dynamoDBClient);
    }

    @Test
    public void shouldReportCompactionJobCreated() {
        // Given
        Partition partition = singlePartition();
        FileInfoFactory fileFactory = new FileInfoFactory(schema, Collections.singletonList(partition), Instant.now());
        CompactionFactory jobFactory = new CompactionFactory(instanceProperties, tableProperties);
        CompactionJob job = jobFactory.createCompactionJob(
                Collections.singletonList(fileFactory.leafFile(100L, "a", "z")),
                partition.getId());

        // When
        store.jobCreated(job);

        // Then TODO
    }

    private Partition singlePartition() {
        return new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct().get(0);
    }
}
