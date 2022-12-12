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
import sleeper.core.partition.Partition;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ClientTestUtils.example;
import static sleeper.status.report.partitions.PartitionStatusReportTestHelper.createRootPartitionWithNoChildren;
import static sleeper.status.report.partitions.PartitionStatusReportTestHelper.createRootPartitionWithTwoChildren;
import static sleeper.status.report.partitions.PartitionStatusReportTestHelper.getStandardReport;

public class PartitionsStatusReportTest {
    @Test
    public void shouldReportNoPartitions() throws IOException {
        // Given
        List<Partition> partitions = Collections.emptyList();
        int splittingPartitionCount = 0;

        // When
        assertThat(getStandardReport(PartitionsQuery.ALL, partitions, splittingPartitionCount)).hasToString(
                example("reports/partitions/noPartitions.txt"));
    }

    @Test
    public void shouldReportRootPartitionWithNoChildren() throws IOException {
        // Given
        List<Partition> partitions = createRootPartitionWithNoChildren();
        int splittingPartitionCount = 0;

        // When
        assertThat(getStandardReport(PartitionsQuery.ALL, partitions, splittingPartitionCount)).hasToString(
                example("reports/partitions/rootWithNoChildren.txt"));
    }

    @Test
    public void shouldReportRootPartitionWithTwoChildren() throws IOException {
        // Given
        List<Partition> partitions = createRootPartitionWithTwoChildren();
        int splittingPartitionCount = 0;

        // When
        assertThat(getStandardReport(PartitionsQuery.ALL, partitions, splittingPartitionCount)).hasToString(
                example("reports/partitions/rootWithTwoChildren.txt"));
    }
}
