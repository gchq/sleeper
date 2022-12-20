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
import sleeper.statestore.StateStore;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ClientTestUtils.example;
import static sleeper.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedPartitions;
import static sleeper.status.report.partitions.PartitionStatusReportTestHelper.createRootPartitionWithNoChildren;
import static sleeper.status.report.partitions.PartitionStatusReportTestHelper.createRootPartitionWithTwoChildrenAboveSplitThreshold;
import static sleeper.status.report.partitions.PartitionStatusReportTestHelper.createRootPartitionWithTwoChildrenBelowSplitThreshold;
import static sleeper.status.report.partitions.PartitionStatusReportTestHelper.createRootPartitionWithTwoChildrenSplitOnByteArray;
import static sleeper.status.report.partitions.PartitionStatusReportTestHelper.createRootPartitionWithTwoChildrenSplitOnLongString;
import static sleeper.status.report.partitions.PartitionStatusReportTestHelper.getStandardReport;

public class PartitionsStatusReportTest {
    @Test
    public void shouldReportNoPartitions() throws Exception {
        // Given
        StateStore store = inMemoryStateStoreWithFixedPartitions(Collections.emptyList());

        // When
        assertThat(getStandardReport(store)).hasToString(
                example("reports/partitions/noPartitions.txt"));
    }

    @Test
    public void shouldReportRootPartitionWithNoChildren() throws Exception {
        // Given
        StateStore store = createRootPartitionWithNoChildren();

        // When
        assertThat(getStandardReport(store)).isEqualTo(
                example("reports/partitions/rootWithNoChildren.txt"));
    }

    @Test
    public void shouldReportRootPartitionWithTwoChildren() throws Exception {
        // Given
        StateStore store = createRootPartitionWithTwoChildrenBelowSplitThreshold();

        // When
        assertThat(getStandardReport(store)).hasToString(
                example("reports/partitions/rootWithTwoChildren.txt"));
    }

    @Test
    public void shouldReportRootPartitionWithTwoChildrenBothNeedSplitting() throws Exception {
        // Given
        StateStore store = createRootPartitionWithTwoChildrenAboveSplitThreshold();

        // When
        assertThat(getStandardReport(store)).hasToString(
                example("reports/partitions/rootWithTwoChildrenBothNeedSplitting.txt"));
    }

    @Test
    public void shouldReportRootPartitionSplitOnByteArray() throws Exception {
        // Given
        StateStore store = createRootPartitionWithTwoChildrenSplitOnByteArray();

        // When
        assertThat(getStandardReport(store)).hasToString(
                example("reports/partitions/rootWithTwoChildrenSplitOnByteArray.txt"));
    }

    @Test
    public void shouldReportRootPartitionSplitOnLongStringHidingMiddle() throws Exception {
        // Given
        StateStore store = createRootPartitionWithTwoChildrenSplitOnLongString();

        // When
        assertThat(getStandardReport(store)).hasToString(
                example("reports/partitions/rootWithTwoChildrenSplitOnLongString.txt"));
    }
}
