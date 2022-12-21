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
package sleeper.status.report.partitions;

import org.junit.Test;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionTree;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedPartitions;
import static sleeper.status.report.partitions.PartitionStatusReportTestHelper.createPartitionsBuilder;
import static sleeper.status.report.partitions.PartitionStatusReportTestHelper.createTableProperties;

public class PartitionsStatusOrderingTest {

    private final TableProperties tableProperties = createTableProperties();

    @Test
    public void shouldOrderPartitionsByTreeLeavesFirst() throws StateStoreException {
        // Given
        PartitionTree partitions = createPartitionsBuilder()
                .leavesWithSplits(Arrays.asList("A", "B", "C"), Arrays.asList("aaa", "bbb"))
                .parentJoining("D", "A", "B")
                .parentJoining("root", "D", "C")
                .buildTree();
        StateStore store = inMemoryStateStoreWithFixedPartitions(
                Stream.of("D", "C", "B", "root", "A")
                        .map(partitions::getPartition).collect(Collectors.toList()));

        // When
        PartitionsStatus status = PartitionsStatus.from(tableProperties, store);

        // Then
        assertThat(status.getPartitions()).extracting("partition.id")
                .containsExactly("A", "B", "C", "D", "root");
    }

    @Test
    public void shouldPutMinAndMaxSideOfPartitionSplitInOrderWhenIdsAreNotInAlphabeticalOrder() throws StateStoreException {
        // Given
        PartitionTree partitions = createPartitionsBuilder()
                .leavesWithSplits(Arrays.asList("some-leaf", "other-leaf"), Collections.singletonList("aaa"))
                .parentJoining("root", "some-leaf", "other-leaf")
                .buildTree();
        StateStore store = inMemoryStateStoreWithFixedPartitions(
                Stream.of("other-leaf", "some-leaf", "root")
                        .map(partitions::getPartition).collect(Collectors.toList()));

        // When
        PartitionsStatus status = PartitionsStatus.from(tableProperties, store);

        // Then
        assertThat(status.getPartitions()).extracting("partition.id")
                .containsExactly("some-leaf", "other-leaf", "root");
    }

    @Test
    public void shouldPutNestedLeavesInOrderOfPartitionSplitSideWhenParentIdsAreNotInAlphabeticalOrder() throws StateStoreException {
        // Given
        PartitionTree partitions = createPartitionsBuilder()
                .leavesWithSplits(
                        Arrays.asList("some-leaf", "other-leaf", "third-leaf", "fourth-leaf"),
                        Arrays.asList("aaa", "bbb", "ccc"))
                .parentJoining("some-middle", "some-leaf", "other-leaf")
                .parentJoining("other-middle", "third-leaf", "fourth-leaf")
                .parentJoining("root", "some-middle", "other-middle")
                .buildTree();
        StateStore store = inMemoryStateStoreWithFixedPartitions(
                Stream.of("other-leaf", "fourth-leaf", "root", "other-middle", "some-leaf", "third-leaf", "some-middle")
                        .map(partitions::getPartition).collect(Collectors.toList()));

        // When
        PartitionsStatus status = PartitionsStatus.from(tableProperties, store);

        // Then
        assertThat(status.getPartitions()).extracting("partition.id").containsExactly(
                "some-leaf", "other-leaf", "third-leaf", "fourth-leaf",
                "some-middle", "other-middle", "root");
    }
}
