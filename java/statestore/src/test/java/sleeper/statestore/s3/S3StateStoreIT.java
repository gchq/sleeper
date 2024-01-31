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
package sleeper.statestore.s3;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class S3StateStoreIT extends S3StateStoreTestBase {
    protected final TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);

    @Test
    void shouldNotReinitialisePartitionsWhenAFileIsPresent() throws Exception {
        // Given
        Schema schema = schemaWithKey("key", new LongType());
        PartitionTree treeBefore = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "before1", "before2", 0L)
                .buildTree();
        PartitionTree treeAfter = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "after1", "after2", 10L)
                .buildTree();
        StateStore stateStore = getStateStore(schema, treeBefore.getAllPartitions());
        stateStore.addFile(FileReferenceFactory.from(treeBefore).partitionFile("before2", 100L));

        // When / Then
        assertThatThrownBy(() -> stateStore.initialise(treeAfter.getAllPartitions()))
                .isInstanceOf(StateStoreException.class);
        assertThat(stateStore.getAllPartitions())
                .containsExactlyInAnyOrderElementsOf(treeBefore.getAllPartitions());
    }

    @Test
    void shouldReinitialisePartitionsWhenNoFilesArePresent() throws Exception {
        // Given
        Schema schema = schemaWithKey("key", new LongType());
        PartitionTree treeBefore = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "before1", "before2", 0L)
                .buildTree();
        PartitionTree treeAfter = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "after1", "after2", 10L)
                .buildTree();
        StateStore stateStore = getStateStore(schema, treeBefore.getAllPartitions());

        // When
        stateStore.initialise(treeAfter.getAllPartitions());

        // Then
        assertThat(stateStore.getAllPartitions())
                .containsExactlyInAnyOrderElementsOf(treeAfter.getAllPartitions());
    }

    private S3StateStore getStateStore(Schema schema,
                                       List<Partition> partitions,
                                       int garbageCollectorDelayBeforeDeletionInMinutes) throws StateStoreException {
        tableProperties.setSchema(schema);
        tableProperties.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, garbageCollectorDelayBeforeDeletionInMinutes);
        S3StateStore stateStore = new S3StateStore(instanceProperties, tableProperties, dynamoDBClient, new Configuration());
        stateStore.initialise(partitions);
        return stateStore;
    }

    private S3StateStore getStateStore(Schema schema,
                                       List<Partition> partitions) throws StateStoreException {
        return getStateStore(schema, partitions, 0);
    }
}
