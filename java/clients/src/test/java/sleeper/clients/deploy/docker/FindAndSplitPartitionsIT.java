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

package sleeper.clients.deploy.docker;

import org.junit.jupiter.api.Test;

import sleeper.clients.docker.FindAndSplitPartitions;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.StateStoreProvider;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.core.testutils.printers.PartitionsPrinter.printPartitions;

public class FindAndSplitPartitionsIT extends DockerInstanceITBase {
    @Test
    void shouldFindAndSplitOnePartition() throws Exception {
        // Given
        String instanceId = UUID.randomUUID().toString().substring(0, 18);
        deployInstance(instanceId, tableProperties -> tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 2));

        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3GivenInstanceId(s3Client, instanceId);
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDB);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDB, instanceProperties, configuration);
        TableProperties tableProperties = tablePropertiesProvider.getByName("system-test");

        List<Record> records = List.of(
                new Record(Map.of("key", "test1")),
                new Record(Map.of("key", "test2")),
                new Record(Map.of("key", "test3")));
        ingestRecords(instanceProperties, tableProperties, records);

        // When
        findAndSplitPartitions(instanceProperties);

        // Then
        Schema schema = tableProperties.getSchema();
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        PartitionTree expectedPartitions = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "test2")
                .buildTree();
        assertThat(printPartitions(schema, new PartitionTree(stateStore.getAllPartitions())))
                .isEqualTo(printPartitions(schema, expectedPartitions));
    }

    @Test
    void shouldIgnorePartitionThatDoesNotNeedSplitting() throws Exception {
        // Given
        String instanceId = UUID.randomUUID().toString().substring(0, 18);
        deployInstance(instanceId, tableProperties -> tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 2));

        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3GivenInstanceId(s3Client, instanceId);
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDB);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDB, instanceProperties, configuration);
        TableProperties tableProperties = tablePropertiesProvider.getByName("system-test");

        List<Record> records = List.of(
                new Record(Map.of("key", "test1")));
        ingestRecords(instanceProperties, tableProperties, records);

        // When
        findAndSplitPartitions(instanceProperties);

        // Then
        Schema schema = tableProperties.getSchema();
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        PartitionTree expectedPartitions = new PartitionsBuilder(schema).singlePartition("root").buildTree();
        assertThat(printPartitions(schema, new PartitionTree(stateStore.getAllPartitions())))
                .isEqualTo(printPartitions(schema, expectedPartitions));
    }

    private void findAndSplitPartitions(InstanceProperties instanceProperties) throws Exception {
        new FindAndSplitPartitions(instanceProperties,
                new TablePropertiesProvider(instanceProperties, s3Client, dynamoDB),
                new StateStoreProvider(dynamoDB, instanceProperties, configuration),
                configuration).run();
    }
}
