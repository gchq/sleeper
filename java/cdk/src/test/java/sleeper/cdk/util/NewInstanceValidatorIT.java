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

package sleeper.cdk.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.cdk.util.ValidatorTestHelper.setupTablesPropertiesFile;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

class NewInstanceValidatorIT extends LocalStackTestBase {

    @TempDir
    public Path temporaryFolder;

    private final InstanceProperties instanceProperties = createTestInstanceProperties();

    @Test
    void shouldNotThrowAnErrorWhenNoBucketsOrTablesExist() throws IOException {
        // Given
        setupTablesPropertiesFile(temporaryFolder, "example-table");

        // When / Then
        assertThatCode(this::validate)
                .doesNotThrowAnyException();
    }

    @Test
    void shouldThrowAnErrorWhenDataBucketExists() throws IOException {
        // Given
        setupTablesPropertiesFile(temporaryFolder, "example-table");
        createBucket(instanceProperties.get(DATA_BUCKET));

        // When / Then
        assertThatThrownBy(this::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Sleeper data bucket exists: " + instanceProperties.get(DATA_BUCKET));
    }

    @Test
    void shouldThrowAnErrorWhenTheQueryResultsBucketExists() throws IOException {
        // Given
        setupTablesPropertiesFile(temporaryFolder, "example-table");
        createBucket(instanceProperties.get(QUERY_RESULTS_BUCKET));

        // When / Then
        assertThatThrownBy(this::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Sleeper query results bucket exists: " + instanceProperties.get(QUERY_RESULTS_BUCKET));
    }

    @Test
    void shouldThrowAnErrorWhenTransactionLogStateStoreExists() throws IOException {
        // Given
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClient).create();
        setupTablesPropertiesFile(temporaryFolder, "example-table");

        // When
        assertThatThrownBy(this::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Sleeper state store table exists: ");
    }

    private void validate() throws IOException {
        Path instancePropertiesPath = temporaryFolder.resolve("instance.properties");
        Files.writeString(instancePropertiesPath, instanceProperties.saveAsString());
        new NewInstanceValidator(s3ClientV2, dynamoClientV2).validate(instanceProperties, instancePropertiesPath);
    }
}
