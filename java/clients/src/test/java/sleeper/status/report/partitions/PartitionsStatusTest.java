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
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;
import static sleeper.status.report.partitions.PartitionStatusReportTestHelper.createRootPartitionWithTwoChildren;
import static sleeper.status.report.partitions.PartitionStatusReportTestHelper.createRootPartitionWithTwoChildrenAboveSplitThreshold;
import static sleeper.status.report.partitions.PartitionStatusReportTestHelper.createRootPartitionWithTwoChildrenBelowSplitThreshold;
import static sleeper.status.report.partitions.PartitionStatusReportTestHelper.createTableProperties;

public class PartitionsStatusTest {

    private final TableProperties tableProperties = createTableProperties();

    @Test
    public void shouldCountRecordsInPartitions() throws StateStoreException {
        // Given
        StateStore store = createRootPartitionWithTwoChildren()
                .partitionFileWithRecords("A", "file-a1.parquet", 5)
                .partitionFileWithRecords("A", "file-a2.parquet", 10)
                .partitionFileWithRecords("B", "file-b.parquet", 5)
                .buildStateStore();

        // When
        PartitionsStatus status = PartitionsStatus.from(tableProperties, store);

        // Then
        assertThat(status.getPartitions())
                .extracting("partition.id", "numberOfFiles", "numberOfRecords")
                .containsExactlyInAnyOrder(tuple("parent", 0, 0L), tuple("A", 2, 15L), tuple("B", 1, 5L));
    }

    @Test
    public void shouldCountLeafPartitions() throws StateStoreException {
        // Given
        StateStore store = createRootPartitionWithTwoChildren().buildStateStore();

        // When
        PartitionsStatus status = PartitionsStatus.from(tableProperties, store);

        // Then
        assertThat(status.getNumLeafPartitions()).isEqualTo(2);
    }

    @Test
    public void shouldFindNoSplittingPartitionsWhenThresholdNotExceeded() throws StateStoreException {
        // Given
        StateStore store = createRootPartitionWithTwoChildrenBelowSplitThreshold();

        // When
        PartitionsStatus status = PartitionsStatus.from(tableProperties, store);

        // Then
        assertThat(status.getNumSplittingPartitions()).isZero();
    }

    @Test
    public void shouldFindSplittingPartitionsWhenThresholdExceeded() throws StateStoreException {
        // Given
        StateStore store = createRootPartitionWithTwoChildrenAboveSplitThreshold();

        // When/Then
        PartitionsStatus status = PartitionsStatus.from(tableProperties, store);
        assertThat(status.getNumSplittingPartitions()).isEqualTo(2);
    }
}
