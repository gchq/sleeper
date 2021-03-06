/*
 * Copyright 2022 Crown Copyright
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
package sleeper.configuration.properties.table;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import sleeper.configuration.properties.SystemDefinedInstanceProperty;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class TablePropertiesIT {
    @ClassRule
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3);

    private static final Schema KEY_VALUE_SCHEMA = new Schema();

    static {
        KEY_VALUE_SCHEMA.setRowKeyFields(new Field("key", new StringType()));
        KEY_VALUE_SCHEMA.setValueFields(new Field("value", new StringType()));
    }

    private AmazonS3 getS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .build();
    }

    private TableProperties createValidProperties() {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(SystemDefinedInstanceProperty.CONFIG_BUCKET, "config");
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, "test");
        tableProperties.setSchema(KEY_VALUE_SCHEMA);
        return tableProperties;
    }

    @Test
    public void shouldSaveToS3() throws IOException {
        // Given
        TableProperties validProperties = createValidProperties();
        AmazonS3 s3Client = getS3Client();
        s3Client.createBucket("config");

        // When
        validProperties.saveToS3(s3Client);

        // Then
        assertTrue(s3Client.doesObjectExist("config", "tables/test"));
    }

    @Test
    public void shouldLoadFromS3() throws IOException {
        // Given
        TableProperties validProperties = createValidProperties();
        AmazonS3 s3Client = getS3Client();
        s3Client.createBucket("config");
        validProperties.saveToS3(s3Client);

        // When
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(SystemDefinedInstanceProperty.CONFIG_BUCKET, "config");
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.loadFromS3(s3Client, "test");

        // Then
        assertEquals(tableProperties, validProperties);
    }
}
