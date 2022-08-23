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
package sleeper.status.report.filestatus;

import com.google.common.io.Resources;
import org.junit.Test;
import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.statestore.FileInfo;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.io.Resources.getResource;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;

public class FileStatusCollectorTest {

    @Test
    public void shouldCollectFileStatusForOneActiveFilePerLeafPartition() throws Exception {
        // Given
        Instant lastStateStoreUpdate = Instant.parse("2022-08-22T14:20:00.001Z");
        Schema schema = Schema.builder().rowKeyFields(new Field("key1", new StringType())).build();
        List<Partition> partitions = PartitionsFromSplitPoints.sequentialIds(schema,
                Arrays.asList("aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg"));
        List<FileInfo> activeFiles = oneFilePerLeafPartition(schema, partitions, 50000000, lastStateStoreUpdate);

        // When
        FileStatus status = FileStatusCollector.run(StateStoreFiles.builder()
                .partitions(partitions).active(activeFiles)
                .readyForGC(StateStoreReadyForGC.none())
                .build());

        // Then
        assertThat(status.verboseReportString(StandardFileStatusReporter::new))
                .isEqualTo(example("reports/standard/oneActiveFilePerLeaf.txt"));
        assertThatJson(status.verboseReportString(JsonFileStatusReporter::new))
                .isEqualTo(example("reports/json/oneActiveFilePerLeaf.json"));
    }

    private static List<FileInfo> oneFilePerLeafPartition(Schema schema, List<Partition> partitions, long recordsPerFile, Instant lastStateStoreUpdate) {
        return partitions.stream()
                .filter(Partition::isLeafPartition)
                .map(partition -> fileForPartition(schema, partition, recordsPerFile, lastStateStoreUpdate))
                .collect(Collectors.toList());
    }

    private static FileInfo fileForPartition(Schema schema, Partition partition, long records, Instant lastStateStoreUpdate) {
        FileInfo file = new FileInfo();
        file.setRowKeyTypes(partition.getRowKeyTypes());
        file.setMinRowKey(minRowKey(schema, partition));
        file.setMaxRowKey(maxRowKey(schema, partition));
        file.setFilename(partition.getId() + ".parquet");
        file.setPartitionId(partition.getId());
        file.setNumberOfRecords(records);
        file.setFileStatus(FileInfo.FileStatus.ACTIVE);
        file.setLastStateStoreUpdateTime(lastStateStoreUpdate.toEpochMilli());
        return file;
    }

    private static Key minRowKey(Schema schema, Partition partition) {
        Region region = partition.getRegion();
        return Key.create(schema.getRowKeyFieldNames().stream()
                .map(field -> region.getRange(field).getMin())
                .collect(Collectors.toList()));
    }

    private static Key maxRowKey(Schema schema, Partition partition) {
        Region region = partition.getRegion();
        return Key.create(schema.getRowKeyFieldNames().stream()
                .map(field -> region.getRange(field).getMax())
                .collect(Collectors.toList()));
    }

    private static String example(String path) throws IOException {
        return Resources.toString(getResource(path), Charset.defaultCharset());
    }
}
