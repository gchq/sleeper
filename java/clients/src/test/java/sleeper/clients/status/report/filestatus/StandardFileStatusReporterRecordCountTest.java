/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.clients.status.report.filestatus;

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.core.statestore.SplitFileInfo;
import sleeper.core.statestore.StateStore;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedPartitions;

public class StandardFileStatusReporterRecordCountTest {

    @Test
    public void shouldReportExactCountWhenLowerThan1K() throws Exception {
        // Given
        FileStatus status = statusWithRecordCount(123);

        // When / Then
        assertThat(status.verboseReportString(StandardFileStatusReporter::new))
                .contains("Total number of records in all active files = 123" + System.lineSeparator());
    }

    @Test
    public void shouldReportKCountWhenLowerThan1M() throws Exception {
        // Given
        FileStatus status = statusWithRecordCount(123456);

        // When / Then
        assertThat(status.verboseReportString(StandardFileStatusReporter::new))
                .contains("Total number of records in all active files = 123K (123,456)" + System.lineSeparator());
    }

    @Test
    public void shouldReportMCountWhenLowerThan1G() throws Exception {
        // Given
        FileStatus status = statusWithRecordCount(123_456_789);

        // When / Then
        assertThat(status.verboseReportString(StandardFileStatusReporter::new))
                .contains("Total number of records in all active files = 123M (123,456,789)" + System.lineSeparator());
    }

    @Test
    public void shouldReportGCountWhenHigherThan1G() throws Exception {
        // Given
        FileStatus status = statusWithRecordCount(123_123_456_789L);

        // When / Then
        assertThat(status.verboseReportString(StandardFileStatusReporter::new))
                .contains("Total number of records in all active files = 123G (123,123,456,789)" + System.lineSeparator());
    }

    @Test
    public void shouldReportTCountWhenHigherThan1T() throws Exception {
        // Given
        FileStatus status = statusWithRecordCount(123_456_123_456_789L);

        // When / Then
        assertThat(status.verboseReportString(StandardFileStatusReporter::new))
                .contains("Total number of records in all active files = 123T (123,456,123,456,789)" + System.lineSeparator());
    }

    @Test
    public void shouldReportTCountWhenHigherThan1000T() throws Exception {
        // Given
        FileStatus status = statusWithRecordCount(1_234_123_123_456_789L);

        // When / Then
        assertThat(status.verboseReportString(StandardFileStatusReporter::new))
                .contains("Total number of records in all active files = 1,234T (1,234,123,123,456,789)" + System.lineSeparator());
    }

    @Test
    public void shouldRoundUpKCount() throws Exception {
        // Given
        FileStatus status = statusWithRecordCount(123_500);

        // When / Then
        assertThat(status.verboseReportString(StandardFileStatusReporter::new))
                .contains("Total number of records in all active files = 124K (123,500)" + System.lineSeparator());
    }

    @Test
    public void shouldRoundUpMCount() throws Exception {
        // Given
        FileStatus status = statusWithRecordCount(123_500_000);

        // When / Then
        assertThat(status.verboseReportString(StandardFileStatusReporter::new))
                .contains("Total number of records in all active files = 124M (123,500,000)" + System.lineSeparator());
    }

    @Test
    public void shouldRoundUpGCount() throws Exception {
        // Given
        FileStatus status = statusWithRecordCount(123_500_000_000L);

        // When / Then
        assertThat(status.verboseReportString(StandardFileStatusReporter::new))
                .contains("Total number of records in all active files = 124G (123,500,000,000)" + System.lineSeparator());
    }

    @Test
    public void shouldRoundUpTCount() throws Exception {
        // Given
        FileStatus status = statusWithRecordCount(123_500_000_000_000L);

        // When / Then
        assertThat(status.verboseReportString(StandardFileStatusReporter::new))
                .contains("Total number of records in all active files = 124T (123,500,000,000,000)" + System.lineSeparator());
    }

    @Test
    public void shouldAddSuffixIfRecordCountIncludesApproximates() throws Exception {
        // Given
        Instant lastStateStoreUpdate = Instant.parse("2022-08-22T14:20:00.001Z");
        PartitionTree partitions = new PartitionsBuilder(schemaWithKey("key1", new StringType()))
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "aaa")
                .buildTree();
        FileInfo file1 = FileInfoFactory.fromUpdatedAt(partitions, lastStateStoreUpdate).rootFile(1000);
        FileInfo file2 = SplitFileInfo.copyToChildPartition(file1, "L", "file2.parquet");
        FileInfo file3 = SplitFileInfo.copyToChildPartition(file1, "R", "file3.parquet");
        StateStore stateStore = inMemoryStateStoreWithFixedPartitions(partitions.getAllPartitions());
        stateStore.addFiles(List.of(file2, file3));
        FileStatus status = new FileStatusCollector(stateStore).run(100);

        // When / Then
        assertThat(status.verboseReportString(StandardFileStatusReporter::new))
                .contains("Total number of records in all active files (approx) = 1K (1,000)" + System.lineSeparator()
                        + "Total number of records in leaf partitions (approx) = 1K (1,000)" + System.lineSeparator()
                        + "Percentage of records in leaf partitions (approx) = 100.0");
    }

    private static FileStatus statusWithRecordCount(long recordCount) throws Exception {
        Instant lastStateStoreUpdate = Instant.parse("2022-08-22T14:20:00.001Z");
        PartitionTree partitions = new PartitionsBuilder(schemaWithKey("key1", new StringType()))
                .singlePartition("root").buildTree();
        StateStore stateStore = inMemoryStateStoreWithFixedPartitions(partitions.getAllPartitions());
        stateStore.addFile(FileInfoFactory.fromUpdatedAt(partitions, lastStateStoreUpdate).rootFile(recordCount));

        return new FileStatusCollector(stateStore).run(100);
    }
}
