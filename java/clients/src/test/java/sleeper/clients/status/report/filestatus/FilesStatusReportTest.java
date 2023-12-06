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

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.core.statestore.SplitFileInfo;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;

public class FilesStatusReportTest {
    private final Schema schema = Schema.builder().rowKeyFields(new Field("key1", new StringType())).build();
    private final Instant lastStateStoreUpdate = Instant.parse("2022-08-22T14:20:00.001Z");

    @Test
    public void shouldReportFilesStatusGivenOneActiveFilePerLeafPartition() throws Exception {
        // Given
        List<Partition> partitions = new PartitionsBuilder(schema)
                .rootFirst("0")
                .splitToNewChildren("0", "1", "H", "ggg")
                .splitToNewChildren("1", "2", "G", "fff")
                .splitToNewChildren("2", "3", "F", "eee")
                .splitToNewChildren("3", "4", "E", "ddd")
                .splitToNewChildren("4", "5", "D", "ccc")
                .splitToNewChildren("5", "6", "C", "bbb")
                .splitToNewChildren("6", "A", "B", "aaa")
                .buildList();
        FileInfoFactory fileInfoFactory = FileInfoFactory.fromUpdatedAt(schema, partitions, lastStateStoreUpdate);
        List<FileInfo> activeFiles = List.of(
                fileInfoFactory.partitionFile("A", 50000001),
                fileInfoFactory.partitionFile("B", 50000002),
                fileInfoFactory.partitionFile("C", 50000003),
                fileInfoFactory.partitionFile("D", 50000004),
                fileInfoFactory.partitionFile("E", 50000005),
                fileInfoFactory.partitionFile("F", 50000006),
                fileInfoFactory.partitionFile("G", 50000007),
                fileInfoFactory.partitionFile("H", 50000008));

        // When
        FileStatus status = FileStatusCollector.run(StateStoreSnapshot.builder()
                .partitions(partitions).active(activeFiles)
                .readyForGC(StateStoreReadyForGC.none())
                .build());

        // Then
        assertThat(status.verboseReportString(StandardFileStatusReporter::new))
                .isEqualTo(example("reports/filestatus/standard/oneActiveFilePerLeaf.txt"));
        assertThatJson(status.verboseReportString(JsonFileStatusReporter::new))
                .isEqualTo(example("reports/filestatus/json/oneActiveFilePerLeaf.json"));
    }

    @Test
    public void shouldReportFilesStatusGivenActiveFileInLeafAndMiddlePartition() throws Exception {
        // Given
        List<Partition> partitions = new PartitionsBuilder(schema)
                .rootFirst("A")
                .splitToNewChildren("A", "B", "C", "mmm")
                .splitToNewChildren("B", "D", "E", "ggg")
                .buildList();
        FileInfoFactory fileInfoFactory = FileInfoFactory.fromUpdatedAt(schema, partitions, lastStateStoreUpdate);
        List<FileInfo> activeFiles = Arrays.asList(
                fileInfoFactory.partitionFile("D", 50000001),
                fileInfoFactory.partitionFile("B", 50000002));

        // When
        FileStatus status = FileStatusCollector.run(StateStoreSnapshot.builder()
                .partitions(partitions).active(activeFiles)
                .readyForGC(StateStoreReadyForGC.none())
                .build());

        // Then
        assertThat(status.verboseReportString(StandardFileStatusReporter::new))
                .isEqualTo(example("reports/filestatus/standard/leafAndMiddleFile.txt"));
        assertThatJson(status.verboseReportString(JsonFileStatusReporter::new))
                .isEqualTo(example("reports/filestatus/json/leafAndMiddleFile.json"));
    }

    @Test
    public void shouldReportFilesStatusWhenSomeFilesHaveBeenSplit() throws Exception {
        // Given
        List<Partition> partitions = new PartitionsBuilder(schema)
                .rootFirst("A")
                .splitToNewChildren("A", "B", "C", "mmm")
                .buildList();
        FileInfoFactory fileInfoFactory = FileInfoFactory.fromUpdatedAt(schema, partitions, lastStateStoreUpdate);
        FileInfo rootFile = fileInfoFactory.partitionFile("A", "not-split.parquet", 1000);
        FileInfo oldFile = fileInfoFactory.partitionFile("A", "split.parquet", 1000);
        FileInfo newFile1 = SplitFileInfo.referenceForChildPartition(oldFile, "B")
                .toBuilder().lastStateStoreUpdateTime(lastStateStoreUpdate).build();
        FileInfo newFile2 = SplitFileInfo.referenceForChildPartition(oldFile, "C")
                .toBuilder().lastStateStoreUpdateTime(lastStateStoreUpdate).build();

        // When
        FileStatus status = FileStatusCollector.run(StateStoreSnapshot.builder()
                .partitions(partitions).active(List.of(rootFile, oldFile, newFile1, newFile2))
                .readyForGC(StateStoreReadyForGC.none())
                .build());

        // Then
        assertThat(status.verboseReportString(StandardFileStatusReporter::new))
                .isEqualTo(example("reports/filestatus/standard/splitFile.txt"));
        assertThatJson(status.verboseReportString(JsonFileStatusReporter::new))
                .isEqualTo(example("reports/filestatus/json/splitFile.json"));
    }

    private static String example(String path) throws IOException {
        URL url = FilesStatusReportTest.class.getClassLoader().getResource(path);
        return IOUtils.toString(Objects.requireNonNull(url), Charset.defaultCharset());
    }

}
