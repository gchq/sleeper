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
package sleeper.systemtest.dsl.statestore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;

import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.testutils.printers.FileReferencePrinter.printFiles;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.MAIN;

@InMemoryDslTest
public class SystemTestStateStoreFakeCommitsTest {

    @BeforeEach
    void setUp(SleeperSystemTest sleeper) {
        sleeper.connectToInstance(MAIN);
    }

    @Test
    void shouldSendOneFileCommit(SleeperSystemTest sleeper) {
        // When
        sleeper.stateStore().fakeCommits().addPartitionFile("root", "file.parquet", 100);

        // Then
        assertThat(printFiles(sleeper.partitioning().tree(), sleeper.tableFiles().all()))
                .isEqualTo("Unreferenced files: 0\n" +
                        "Referenced files: 1\n" +
                        "File references: 1\n" +
                        "Partition at root: 100 records in file 1\n");
    }

    @Test
    void shouldSendManyFileCommits(SleeperSystemTest sleeper) {
        // When
        sleeper.stateStore().fakeCommits()
                .sendNumbered(LongStream.rangeClosed(1, 1000),
                        (i, commit) -> commit.addPartitionFile("root", "file-" + i + ".parquet", i));

        // Then
        assertThat(sleeper.tableFiles().references()).hasSize(1000);
    }

}
