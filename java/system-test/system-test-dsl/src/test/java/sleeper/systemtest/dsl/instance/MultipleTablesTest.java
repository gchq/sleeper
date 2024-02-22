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

package sleeper.systemtest.dsl.instance;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.schema.Schema;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;

import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.DEFAULT_SCHEMA;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.withDefaultProperties;

@InMemoryDslTest
public class MultipleTablesTest {
    private final Schema schema = DEFAULT_SCHEMA;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper) {
        sleeper.connectToInstanceNoTables(withDefaultProperties("main"));
    }

    @Test
    void shouldIngestOneFileToMultipleTables(SleeperSystemTest sleeper) {
        // Given we have several tables
        // And we have one source file to be ingested
        sleeper.tables().createMany(5, schema);
        sleeper.sourceFiles().createWithNumberedRecords(schema, "file.parquet", LongStream.range(0, 100));

        // When we send an ingest job with the source file to all tables
        sleeper.ingest().byQueue().sendSourceFilesToAllTables("file.parquet")
                .invokeTask().waitForJobs();

        // Then all tables should contain the source file records
        // And all tables should have one active file
        assertThat(sleeper.query().byQueue().allRecordsByTable())
                .hasSize(5)
                .allSatisfy(((table, records) -> assertThat(records).containsExactlyElementsOf(
                        sleeper.generateNumberedRecords(schema, LongStream.range(0, 100)))));
        assertThat(sleeper.tableFiles().referencesByTable())
                .hasSize(5)
                .allSatisfy((table, files) -> assertThat(files).hasSize(1));
    }

}
