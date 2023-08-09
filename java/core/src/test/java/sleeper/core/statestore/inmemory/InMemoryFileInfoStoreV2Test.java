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

package sleeper.core.statestore.inmemory;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.key.Key;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileDataStatistics;
import sleeper.core.statestore.FileInfoV2;
import sleeper.core.statestore.StateStoreV2;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.statestore.AddFileRequest.addFile;
import static sleeper.core.statestore.AddFilesRequest.addFiles;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreV2WithFixedPartitions;

public class InMemoryFileInfoStoreV2Test {

    @Nested
    @DisplayName("Ingest")
    class Ingest {

        @Test
        public void shouldAddAndReadAFile() {
            // Given
            Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .buildTree();
            StateStoreV2 store = inMemoryStateStoreV2WithFixedPartitions(tree.getAllPartitions());
            FileDataStatistics statistics = FileDataStatistics.builder()
                    .numberOfRecords(12)
                    .minRowKey(Key.create("abc"))
                    .maxRowKey(Key.create("def"))
                    .build();

            // When
            store.finishIngest(addFiles(request -> request
                    .files(List.of(addFile(file -> file
                            .partitionId("root")
                            .filename("test-file.parquet")
                            .statistics(statistics))))));

            // Then
            assertThat(store.getPartitionFiles()).containsExactly(
                    FileInfoV2.builder()
                            .partitionId("root")
                            .filename("test-file.parquet")
                            .statistics(statistics)
                            .build());
        }
    }
}
