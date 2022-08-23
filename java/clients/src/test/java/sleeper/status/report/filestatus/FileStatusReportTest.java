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
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.statestore.FileInfo;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static com.google.common.io.Resources.getResource;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;

public class FileStatusReportTest {

    @Test
    public void shouldCollectFileStatusForOneActiveFilePerLeafPartition() throws Exception {
        // Given
        Instant lastStateStoreUpdate = Instant.parse("2022-08-22T14:20:00.001Z");
        Schema schema = Schema.builder().rowKeyFields(new Field("key1", new StringType())).build();
        List<Partition> partitions = PartitionsFromSplitPoints.sequentialIds(schema,
                Arrays.asList("aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg"));
        FileInfoFactory fileInfoFactory = new FileInfoFactory(schema, partitions, lastStateStoreUpdate);
        List<FileInfo> activeFiles = Arrays.asList(
                fileInfoFactory.leafFile(50000001, "123", "456"),
                fileInfoFactory.leafFile(50000002, "abc", "az"),
                fileInfoFactory.leafFile(50000003, "bcd", "bz"),
                fileInfoFactory.leafFile(50000004, "cde", "cz"),
                fileInfoFactory.leafFile(50000005, "def", "dz"),
                fileInfoFactory.leafFile(50000006, "efg", "ez"),
                fileInfoFactory.leafFile(50000007, "fgh", "fz"),
                fileInfoFactory.leafFile(50000008, "ghi", "gz"));

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

    @Test
    public void shouldCollectFileStatusForFileInLeafAndMiddlePartition() throws Exception {
        // Given
        Instant lastStateStoreUpdate = Instant.parse("2022-08-22T14:20:00.001Z");
        Schema schema = Schema.builder().rowKeyFields(new Field("key1", new StringType())).build();
        List<Partition> partitions = PartitionsFromSplitPoints.sequentialIds(schema,
                Arrays.asList("beeblebrox", "wowbagger"));
        FileInfoFactory fileInfoFactory = new FileInfoFactory(schema, partitions, lastStateStoreUpdate);
        List<FileInfo> activeFiles = Arrays.asList(
                fileInfoFactory.leafFile(50000001, "aardvark", "arthur"),
                fileInfoFactory.middleFile(50000002, "arthur dent", "beeblebrox, zaphod"));

        // When
        FileStatus status = FileStatusCollector.run(StateStoreFiles.builder()
                .partitions(partitions).active(activeFiles)
                .readyForGC(StateStoreReadyForGC.none())
                .build());

        // Then
        assertThat(status.verboseReportString(StandardFileStatusReporter::new))
                .isEqualTo(example("reports/standard/leafAndMiddleFile.txt"));
        assertThatJson(status.verboseReportString(JsonFileStatusReporter::new))
                .isEqualTo(example("reports/json/leafAndMiddleFile.json"));
    }

    private static String example(String path) throws IOException {
        return Resources.toString(getResource(path), Charset.defaultCharset());
    }

}
