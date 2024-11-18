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
package sleeper.compaction.core.job.dispatch;

import org.approvaltests.Approvals;
import org.approvaltests.core.Options;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;

public class CompactionJobDispatchRequestSerDeTest {

    CompactionJobDispatchRequestSerDe serDe = new CompactionJobDispatchRequestSerDe();
    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);

    @Test
    void shouldConvertRequestToFromJson() {
        // Given
        instanceProperties.set(DATA_BUCKET, "test-bucket");
        tableProperties.set(TABLE_ID, "test-table");
        CompactionJobDispatchRequest request = CompactionJobDispatchRequest.forTableWithBatchIdAtTime(
                instanceProperties, tableProperties, "test-batch", Instant.parse("2024-11-18T12:01:00Z"));

        // When
        String json = serDe.toJsonPrettyPrint(request);

        // Then
        assertThat(serDe.fromJson(json)).isEqualTo(request);
        Approvals.verify(json, new Options().forFile().withExtension(".json"));
    }
}
