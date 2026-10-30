/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.compaction.core.job;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Region;
import sleeper.core.schema.Schema;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class CompactionRequestTest {

    Schema schema = createSchemaWithKey("key");
    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    CompactionJob job = CompactionJob.builder()
            .tableId(tableProperties.get(TABLE_ID))
            .jobId("test-job")
            .partitionId("root")
            .inputFiles(List.of("input.parquet"))
            .outputFile("output.parquet")
            .build();

    @Test
    void shouldUseDefaultProgressCallbackIntervalWhenNotSet() {
        // When
        CompactionRequest request = requestBuilder().build();

        // Then
        assertThat(request.getProgressCallbackInterval()).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void shouldSetProgressCallbackInterval() {
        // When
        CompactionRequest request = requestBuilder()
                .progressCallbackInterval(Duration.ofMillis(100))
                .build();

        // Then
        assertThat(request.getProgressCallbackInterval()).isEqualTo(Duration.ofMillis(100));
    }

    @Test
    void shouldRefuseZeroProgressCallbackInterval() {
        // Given
        CompactionRequest.Builder builder = requestBuilder()
                .progressCallbackInterval(Duration.ZERO);

        // When / Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRefuseNegativeProgressCallbackInterval() {
        // Given
        CompactionRequest.Builder builder = requestBuilder()
                .progressCallbackInterval(Duration.ofMillis(-1));

        // When / Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRefuseProgressCallbackIntervalLessThanOneMillisecond() {
        // Given
        CompactionRequest.Builder builder = requestBuilder()
                .progressCallbackInterval(Duration.ofNanos(500_000));

        // When / Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class);
    }

    private CompactionRequest.Builder requestBuilder() {
        return CompactionRequest.builder()
                .job(job)
                .tableProperties(tableProperties)
                .region(Region.coveringAllValuesOfAllRowKeys(schema));
    }
}
