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

package sleeper.configuration.properties;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.CommonTestConstants;

import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CommonProperty.FORCE_RELOAD_PROPERTIES;
import static sleeper.configuration.properties.instance.CommonProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;

@Testcontainers
public class PropertiesReloaderIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3);

    protected final AmazonS3 s3Client = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());

    @Test
    void shouldReloadPropertiesIfForceReloadPropertiesSetToTrue() throws Exception {
        // Given
        InstanceProperties propertiesBefore = createTestInstanceProperties(s3Client, properties ->
                properties.set(FORCE_RELOAD_PROPERTIES, "true"));
        InstanceProperties propertiesAfter = updatePropertiesInS3(propertiesBefore, properties ->
                properties.set(MAXIMUM_CONNECTIONS_TO_S3, "26"));
        PropertiesReloader reloader = PropertiesReloader.ifConfigured(s3Client, propertiesBefore,
                new TablePropertiesProvider(s3Client, propertiesBefore));

        // When
        reloader.reloadIfNeeded();

        // Then
        assertThat(propertiesBefore).isEqualTo(propertiesAfter);
    }

    @Test
    void shouldNotReloadPropertiesIfForceReloadPropertiesSetToFalse() throws Exception {
        // Given
        InstanceProperties propertiesBefore = createTestInstanceProperties(s3Client, properties ->
                properties.set(FORCE_RELOAD_PROPERTIES, "false"));
        InstanceProperties propertiesAfter = updatePropertiesInS3(propertiesBefore, properties ->
                properties.set(MAXIMUM_CONNECTIONS_TO_S3, "26"));
        PropertiesReloader reloader = PropertiesReloader.ifConfigured(s3Client, propertiesBefore,
                new TablePropertiesProvider(s3Client, propertiesBefore));

        // When
        reloader.reloadIfNeeded();

        // Then
        assertThat(propertiesBefore).isNotEqualTo(propertiesAfter);
    }

    private InstanceProperties updatePropertiesInS3(
            InstanceProperties propertiesBefore, Consumer<InstanceProperties> extraProperties) throws Exception {
        InstanceProperties propertiesAfter = new InstanceProperties();
        propertiesAfter.loadFromS3(s3Client, propertiesBefore.get(CONFIG_BUCKET));
        extraProperties.accept(propertiesAfter);
        propertiesAfter.saveToS3(s3Client);
        return propertiesAfter;
    }
}
