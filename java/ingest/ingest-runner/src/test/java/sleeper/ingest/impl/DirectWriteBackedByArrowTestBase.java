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

package sleeper.ingest.impl;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.properties.validation.IngestFileWritingStrategy;
import sleeper.core.record.Record;
import sleeper.ingest.testutils.IngestCoordinatorTestParameters;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class DirectWriteBackedByArrowTestBase {
    @TempDir
    protected Path temporaryFolder;
    protected final Configuration configuration = new Configuration();

    protected IngestCoordinatorTestParameters.Builder createTestParameterBuilder() {
        return IngestCoordinatorTestParameters.builder()
                .temporaryFolder(temporaryFolder)
                .hadoopConfiguration(configuration)
                .ingestFileWritingStrategy(IngestFileWritingStrategy.ONE_FILE_PER_LEAF);
    }

    static void assertThatRecordsHaveFieldValuesThatAllAppearInRangeInSameOrder(List<Record> records, String fieldName, LongStream range) {
        assertThat(range.boxed())
                .containsSubsequence(records.stream()
                        .mapToLong(record -> (long) record.get(fieldName))
                        .boxed().collect(Collectors.toList()));
    }
}
