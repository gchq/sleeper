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

import sleeper.core.util.S3BucketName;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class S3BucketNameTest {

    private String mockAccountId;

    @BeforeEach
    void setUp() {
        mockAccountId = "123456789012";
    }

    @Test
    void shouldRefuseNamePortionLessThan20CharactersAndBucketNameExceeds63Characters() {

        // Given / When / Then
        assertThatThrownBy(() -> S3BucketName.parse(mockAccountId, "123456789123456789012345678901234567890", "bucket", "name"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Complete bucket name exceeds 63 characters.");
    }

    @Test
    void shouldRefuseNamePortionLongerThan20Characters() {

        // Given / When / Then
        assertThatThrownBy(() -> S3BucketName.parse(mockAccountId, "123456789", "bucket", "name", "longer", "than", "20", "chars"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Name portion exceeds 20 characters.");
    }

}
