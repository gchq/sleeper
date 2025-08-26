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
package sleeper.systemtest.drivers.ingest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.systemtest.configuration.SystemTestDataGenerationJob;
import sleeper.systemtest.configuration.SystemTestDataGenerationJobSerDe;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_BUCKET_NAME;

public class SystemTestDataGenerationJobWriterIT extends LocalStackTestBase {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    SystemTestStandaloneProperties testProperties = new SystemTestStandaloneProperties();

    @BeforeEach
    void setUp() {
        testProperties.set(SYSTEM_TEST_BUCKET_NAME, UUID.randomUUID().toString());
        createBucket(testProperties.get(SYSTEM_TEST_BUCKET_NAME));
    }

    @Test
    void shouldWriteDataGenerationJob() {
        // Given
        SystemTestDataGenerationJob job = SystemTestDataGenerationJob.builder()
                .instanceProperties(instanceProperties)
                .testProperties(testProperties)
                .tableName("test-table")
                .build();

        // When
        String objectKey = writer().writeJobGetObjectKey(job);

        // Then
        assertThat(getJobFromBucket(objectKey))
                .isEqualTo(job);
    }

    private SystemTestDataGenerationJobWriter writer() {
        return new SystemTestDataGenerationJobWriter(testProperties, s3Client);
    }

    private SystemTestDataGenerationJob getJobFromBucket(String objectKey) {
        SystemTestDataGenerationJobSerDe serDe = new SystemTestDataGenerationJobSerDe();
        String json = getObjectAsString(testProperties.get(SYSTEM_TEST_BUCKET_NAME), objectKey);
        return serDe.fromJson(json);
    }

}
