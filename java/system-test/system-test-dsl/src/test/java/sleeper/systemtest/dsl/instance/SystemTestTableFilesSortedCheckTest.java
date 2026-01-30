/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.systemtest.dsl.instance;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.row.testutils.SortedRowsCheck;
import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;

import java.nio.file.Path;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.IN_MEMORY_MAIN;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;

@InMemoryDslTest
public class SystemTestTableFilesSortedCheckTest {

    Path tempDir;

    @BeforeEach
    void setUp(SleeperDsl sleeper) {
        sleeper.connectToInstanceAddOnlineTable(IN_MEMORY_MAIN);
    }

    @Test
    void shouldFindFileIsSortedAfterIngest(SleeperDsl sleeper) {
        // Given
        sleeper.ingest().direct(tempDir).numberedRows(LongStream.of(1, 3, 2));

        // When / Then
        assertThat(sleeper.tableFiles().all().getFilesWithReferences())
                .first().satisfies(file -> {
                    assertThat(SortedRowsCheck.check(DEFAULT_SCHEMA, sleeper.getRows(file)))
                            .isEqualTo(SortedRowsCheck.sorted(3));
                });
    }

    @Test
    void shouldFindFileIsNotSortedAfterAddingSourceFile(SleeperDsl sleeper) throws Exception {
        // Given
        sleeper.sourceFiles().inDataBucket().createWithNumberedRows("test.parquet", LongStream.of(1, 3, 2));
        sleeper.ingest().toStateStore().addFileOnEveryPartition("test.parquet", 3);

        // When / Then
        assertThat(sleeper.tableFiles().all().getFilesWithReferences())
                .first().satisfies(file -> {
                    assertThat(SortedRowsCheck.check(DEFAULT_SCHEMA, sleeper.getRows(file)))
                            .isEqualTo(SortedRowsCheck.outOfOrderAt(3,
                                    sleeper.generateNumberedRows().row(3),
                                    sleeper.generateNumberedRows().row(2)));
                });
    }

}
