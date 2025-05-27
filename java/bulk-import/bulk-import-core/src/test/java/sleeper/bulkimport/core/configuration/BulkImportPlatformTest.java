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
package sleeper.bulkimport.core.configuration;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.OptionalStack;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;

public class BulkImportPlatformTest {

    @Test
    void shouldGetQueueUrl() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL, "some-queue-url");

        // When
        String queueUrl = BulkImportPlatform.EMRServerless.getBulkImportQueueUrl(instanceProperties);

        // Then
        assertThat(queueUrl).isEqualTo("some-queue-url");
    }

    @Test
    void shouldGetOptionalStack() {
        // When
        OptionalStack stack = BulkImportPlatform.EMRServerless.getOptionalStack();

        // Then
        assertThat(stack).isEqualTo(OptionalStack.EmrServerlessBulkImportStack);
    }

    @Test
    void shouldFindPlatformIsEnabled() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.setEnumList(OPTIONAL_STACKS, List.of(OptionalStack.EmrServerlessBulkImportStack));

        // When
        boolean enabled = BulkImportPlatform.EMRServerless.isDeployed(instanceProperties);

        // Then
        assertThat(enabled).isTrue();
    }

    @Test
    void shouldFindPlatformIsNotEnabled() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.setEnumList(OPTIONAL_STACKS, List.<OptionalStack>of());

        // When
        boolean enabled = BulkImportPlatform.EMRServerless.isDeployed(instanceProperties);

        // Then
        assertThat(enabled).isFalse();
    }

}
