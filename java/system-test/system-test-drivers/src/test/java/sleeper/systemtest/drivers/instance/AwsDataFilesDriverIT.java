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
package sleeper.systemtest.drivers.instance;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.record.Record;
import sleeper.systemtest.drivers.testutil.LocalStackDslTest;
import sleeper.systemtest.dsl.SleeperSystemTest;

import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.drivers.testutil.LocalStackTestInstance.LOCALSTACK_MAIN;

@LocalStackDslTest
public class AwsDataFilesDriverIT {

    @BeforeEach
    void setUp(SleeperSystemTest sleeper) {
        sleeper.connectToInstanceAddOnlineTable(LOCALSTACK_MAIN);
    }

    @Test
    void shouldReadFile(SleeperSystemTest sleeper) throws Exception {
        // Given
        sleeper.sourceFiles().inDataBucket()
                .createWithNumberedRecords("test.parquet", LongStream.of(1, 3, 2));
        sleeper.ingest().toStateStore().addFileOnEveryPartition("test.parquet", 3);

        // When / Then
        assertThat(sleeper.tableFiles().all().getFilesWithReferences())
                .first().satisfies(file -> {
                    try (CloseableIterator<Record> iterator = sleeper.getRecords(file)) {
                        assertThat(iterator)
                                .toIterable()
                                .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.of(1, 3, 2)));
                    }
                });
    }

}
