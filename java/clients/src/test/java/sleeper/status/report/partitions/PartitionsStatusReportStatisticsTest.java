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
import sleeper.core.partition.Partition;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.status.report.PartitionsStatusReport;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.MapEntry.entry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static sleeper.status.report.partitions.PartitionStatusReportTestHelper.createFileInfosNonSplitting;
import static sleeper.status.report.partitions.PartitionStatusReportTestHelper.createFileInfosSplitting;
import static sleeper.status.report.partitions.PartitionStatusReportTestHelper.createRootPartitionWithTwoChildren;
import static sleeper.status.report.partitions.PartitionStatusReportTestHelper.createTableProperties;

public class PartitionsStatusReportStatisticsTest {

    private final StateStore store = mock(StateStore.class);
    private final PartitionsStatusReporter reporter = new StandardPartitionsStatusReporter(System.out);
    private final PartitionsQuery query = PartitionsQuery.ALL;
    private final TableProperties tableProperties = createTableProperties();

    @Test
    public void shouldReportPartitionStatisticsWhenThresholdNotExceeded() throws StateStoreException {
        // Given
        List<Partition> partitions = createRootPartitionWithTwoChildren();
        when(query.run(store)).thenReturn(partitions);
        List<FileInfo> allFiles = createFileInfosNonSplitting(partitions);
        when(store.getActiveFiles()).thenReturn(allFiles);

        // When/Then
        PartitionsStatusReport partitionsStatusReport = new PartitionsStatusReport(store, tableProperties, reporter, query);
        assertThat(partitionsStatusReport.getSplittingPartitionCount()).isEqualTo(0);
        assertThat(partitionsStatusReport.getPartitionMapToNumberOfRecords())
                .containsExactly(entry("A", 5L), entry("B", 5L));
    }

    @Test
    public void shouldReportPartitionStatisticsWhenThresholdExceeded() throws StateStoreException {
        // Given
        List<Partition> partitions = createRootPartitionWithTwoChildren();
        when(query.run(store)).thenReturn(partitions);
        List<FileInfo> allFiles = createFileInfosSplitting(partitions);
        when(store.getActiveFiles()).thenReturn(allFiles);

        // When/Then
        PartitionsStatusReport partitionsStatusReport = new PartitionsStatusReport(store, tableProperties, reporter, query);
        assertThat(partitionsStatusReport.getSplittingPartitionCount()).isEqualTo(2);
        assertThat(partitionsStatusReport.getPartitionMapToNumberOfRecords())
                .containsExactly(entry("A", 100L), entry("B", 100L));
    }

    @Test
    public void shouldCountNumberOfRecordsCorrectly() throws StateStoreException {
        // Given
        List<Partition> partitions = createRootPartitionWithTwoChildren();
        when(query.run(store)).thenReturn(partitions);
        List<FileInfo> allFiles = createFileInfosSplitting(partitions);
        when(store.getActiveFiles()).thenReturn(allFiles);

        // When/Then
        PartitionsStatusReport partitionsStatusReport = new PartitionsStatusReport(store, tableProperties, reporter, query);
        assertThat(partitionsStatusReport.getPartitionMapToNumberOfRecords())
                .containsExactly(entry("A", 100L), entry("B", 100L));
    }
}
