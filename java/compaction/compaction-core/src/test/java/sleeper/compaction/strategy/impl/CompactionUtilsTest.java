/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.compaction.strategy.impl;

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.strategy.impl.CompactionUtils.getFilesInAscendingOrder;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class CompactionUtilsTest {
    private final FileReferenceFactory factory = FileReferenceFactory.from(new PartitionsBuilder(schemaWithKey("key"))
            .singlePartition("root").buildTree());

    @Test
    void shouldSortFilesByNumberOfRecordsWithNoDuplicates() {
        // Given
        List<FileReference> files = List.of(
                factory.rootFile(456),
                factory.rootFile(789),
                factory.rootFile(123));

        // When / Then
        assertThat(getFilesInAscendingOrder("test-table", files))
                .containsExactly(
                        factory.rootFile(123),
                        factory.rootFile(456),
                        factory.rootFile(789));

    }

    @Test
    void shouldSortFilesByNumberOfRecordsWithDuplicates() {
        // Given
        List<FileReference> files = List.of(
                factory.rootFile(456),
                factory.rootFile(789),
                factory.rootFile(123),
                factory.rootFile(456),
                factory.rootFile(789),
                factory.rootFile(123));

        // When / Then
        assertThat(getFilesInAscendingOrder("test-table", files))
                .containsExactly(
                        factory.rootFile(123),
                        factory.rootFile(123),
                        factory.rootFile(456),
                        factory.rootFile(456),
                        factory.rootFile(789),
                        factory.rootFile(789));

    }
}
