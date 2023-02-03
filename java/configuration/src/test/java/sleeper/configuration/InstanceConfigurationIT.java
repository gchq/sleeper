/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.configuration;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.SystemDefinedInstanceProperty;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesTestHelper;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.CommonTestConstants;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.InstanceConfiguration.loadFromS3;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;

@Testcontainers
public class InstanceConfigurationIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3);

    private AmazonS3 createS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .build();
    }

    @Test
    void shouldLoadInstancePropertiesFromS3() throws IOException {
        // Given
        AmazonS3 s3Client = createS3Client();
        s3Client.createBucket("sleeper-test-instance-config");

        InstanceProperties properties = createTestInstanceProperties();
        properties.set(UserDefinedInstanceProperty.ID, "test-instance");
        properties.set(SystemDefinedInstanceProperty.CONFIG_BUCKET, "sleeper-test-instance-config");
        properties.saveToS3(s3Client);

        // When
        InstanceConfiguration configuration = loadFromS3(s3Client, "test-instance");

        // Then
        assertThat(configuration.getInstanceProperties())
                .isEqualTo(properties);
    }

    @Test
    void shouldLoadTablePropertiesFromS3() throws IOException {
        // Given
        AmazonS3 s3Client = createS3Client();
        s3Client.createBucket("sleeper-test-instance-config");

        InstanceProperties properties = createTestInstanceProperties();
        properties.set(UserDefinedInstanceProperty.ID, "test-instance");
        properties.set(SystemDefinedInstanceProperty.CONFIG_BUCKET, "sleeper-test-instance-config");
        properties.saveToS3(s3Client);
        Schema schema1 = Schema.builder().rowKeyFields(new Field("key1", new StringType())).build();
        TableProperties table1 = TablePropertiesTestHelper.createTestTableProperties(properties, schema1);
        table1.set(TableProperty.TABLE_NAME, "test-table-1");
        table1.saveToS3(s3Client);
        Schema schema2 = Schema.builder().rowKeyFields(new Field("key2", new LongType())).build();
        TableProperties table2 = TablePropertiesTestHelper.createTestTableProperties(properties, schema2);
        table2.set(TableProperty.TABLE_NAME, "test-table-2");
        table2.saveToS3(s3Client);

        // When
        InstanceConfiguration configuration = loadFromS3(s3Client, "test-instance");

        // Then
        assertThat(configuration.getTables())
                .containsExactly(table1, table2);
    }

    @Test
    void shouldLoadNoTablePropertiesFromS3WhenNoneAreSaved() throws IOException {
        // Given
        AmazonS3 s3Client = createS3Client();
        s3Client.createBucket("sleeper-test-instance-config");

        InstanceProperties properties = createTestInstanceProperties();
        properties.set(UserDefinedInstanceProperty.ID, "test-instance");
        properties.set(SystemDefinedInstanceProperty.CONFIG_BUCKET, "sleeper-test-instance-config");
        properties.saveToS3(s3Client);

        // When
        InstanceConfiguration configuration = loadFromS3(s3Client, "test-instance");

        // Then
        assertThat(configuration.getTables())
                .isEmpty();
    }
}
