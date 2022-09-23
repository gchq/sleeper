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
package sleeper.compaction.status.job.testutils;

import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import org.assertj.core.api.AbstractListAssert;
import org.assertj.core.api.ObjectAssert;
import org.junit.After;
import org.junit.Before;
import sleeper.compaction.job.CompactionJobFactory;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.status.job.DynamoDBCompactionJobStatusStore;
import sleeper.compaction.status.job.DynamoDBCompactionJobStatusStoreCreator;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.schema.Schema;
import sleeper.statestore.FileInfoFactory;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusStore.jobStatusTableName;
import static sleeper.compaction.status.job.testutils.CompactionStatusStoreTestUtils.createInstanceProperties;
import static sleeper.compaction.status.job.testutils.CompactionStatusStoreTestUtils.createSchema;
import static sleeper.compaction.status.job.testutils.CompactionStatusStoreTestUtils.createTableProperties;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class DynamoDBCompactionJobStatusStoreTestBase extends DynamoDBTestBase {

    private final InstanceProperties instanceProperties = createInstanceProperties();
    private final String jobStatusTableName = jobStatusTableName(instanceProperties.get(ID));
    private final Schema schema = createSchema();
    private final TableProperties tableProperties = createTableProperties(schema, instanceProperties);

    protected final String tableName = tableProperties.get(TABLE_NAME);
    protected final CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, tableProperties);
    protected final CompactionJobStatusStore store = DynamoDBCompactionJobStatusStore.from(dynamoDBClient, instanceProperties);

    @Before
    public void setUp() {
        DynamoDBCompactionJobStatusStoreCreator.create(instanceProperties, dynamoDBClient);
    }

    @After
    public void tearDown() {
        dynamoDBClient.deleteTable(jobStatusTableName);
    }

    protected Partition singlePartition() {
        return new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct().get(0);
    }

    protected FileInfoFactory fileFactory(Partition singlePartition) {
        return fileFactory(Collections.singletonList(singlePartition));
    }

    protected FileInfoFactory fileFactoryWithPartitions(Consumer<PartitionsBuilder> partitionConfig) {
        PartitionsBuilder builder = new PartitionsBuilder(schema);
        partitionConfig.accept(builder);
        return fileFactory(builder.buildList());
    }

    private FileInfoFactory fileFactory(List<Partition> partitions) {
        return new FileInfoFactory(schema, partitions, Instant.now());
    }

    protected AbstractListAssert<?, List<? extends AssertDynamoDBRecord>, AssertDynamoDBRecord, ObjectAssert<AssertDynamoDBRecord>> assertThatItemsInTable() {
        return assertThat(dynamoDBClient.scan(new ScanRequest().withTableName(jobStatusTableName)).getItems())
                .extracting(AssertDynamoDBJobStatusRecord::actualIgnoringUpdateTime);
    }
}
