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

package sleeper.ingest.runner.impl;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.IngestFileWritingStrategy;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.ingest.runner.testutils.IngestCoordinatorTestParameters;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;

public abstract class DirectWriteBackedByArrowTestBase {
    @TempDir
    protected Path temporaryFolder;
    protected final Configuration configuration = new Configuration();
    protected final InstanceProperties instanceProperties = createTestInstanceProperties();
    protected final TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);
    protected final StateStore stateStore = InMemoryTransactionLogStateStore.create(tableProperties, new InMemoryTransactionLogs());

    protected IngestCoordinatorTestParameters.Builder createTestParameterBuilder() throws Exception {
        return IngestCoordinatorTestParameters.builder()
                .localDataPath(Files.createTempDirectory(temporaryFolder, null).toString())
                .localWorkingDir(Files.createTempDirectory(temporaryFolder, null).toString())
                .hadoopConfiguration(configuration)
                .ingestFileWritingStrategy(IngestFileWritingStrategy.ONE_FILE_PER_LEAF)
                .schema(tableProperties.getSchema())
                .stateStore(stateStore);
    }

    static void assertThatRecordsHaveFieldValuesThatAllAppearInRangeInSameOrder(List<Record> records, String fieldName, LongStream range) {
        assertThat(range.boxed())
                .containsSubsequence(records.stream()
                        .mapToLong(record -> (long) record.get(fieldName))
                        .boxed().collect(Collectors.toList()));
    }
}
