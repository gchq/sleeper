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
package sleeper.cdk.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.ACCOUNT;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstancePropertiesWithId;

public class S3BucketNameTest {

    private final InstanceProperties instanceProperties = createTestInstancePropertiesWithId("my-instance");

    @BeforeEach
    void setUp() {
        instanceProperties.set(ACCOUNT, "my-account");
    }

    @Test
    void shouldCreateBucketName() {
        // When / Then
        assertThat(S3BucketName.create(instanceProperties, "test"))
                .isEqualTo("sleeper-my-instance-test-my-account");
    }

    @Test
    void shouldRefuseNamePortionLessThan20CharactersAndBucketNameExceeds63Characters() {
        // Given sleeper-my-instance-test- is 25 characters
        // And I add 39 more characters to get a total of 64
        instanceProperties.set(ACCOUNT, "123456789012345678901234567890123456789");

        // When / Then
        assertThatThrownBy(() -> S3BucketName.create(instanceProperties, "test"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Complete bucket name exceeds 63 characters.");
    }

    @Test
    void shouldRefuseNamePortionLongerThan20Characters() {
        // Given a name portion 21 characters long
        String namePortion = "123456789012345678901";

        // When / Then
        assertThatThrownBy(() -> S3BucketName.create(instanceProperties, namePortion))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Name portion exceeds 20 characters.");
    }

}
