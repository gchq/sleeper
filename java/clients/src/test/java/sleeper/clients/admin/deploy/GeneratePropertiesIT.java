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
package sleeper.clients.admin.deploy;

import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.regions.Region;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.local.SaveLocalProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;

import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ACCOUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

@Testcontainers
public class GeneratePropertiesIT {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.STS);
    private final AWSSecurityTokenService sts = AWSSecurityTokenServiceClientBuilder.standard()
            .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.STS))
            .withCredentials(localStackContainer.getDefaultCredentialsProvider())
            .build();

    @TempDir
    private Path tempDir;

    @Test
    void shouldGenerateInstancePropertiesCorrectly() {
        // Given/When
        InstanceProperties properties = generateInstancePropertiesBuilder()
                .sts(sts).regionProvider(() -> Region.of(localStackContainer.getRegion()))
                .build().generate();

        // Then
        assertThat(properties.get(ACCOUNT)).isEqualTo(sts.getCallerIdentity(new GetCallerIdentityRequest()).getAccount());
        assertThat(properties.get(REGION)).isEqualTo(localStackContainer.getRegion());
    }

    @Test
    void shouldSaveBucketNamesToLocalDirectoryWhenInstancePropertiesGenerated() throws IOException {
        // Given
        InstanceProperties properties = generateInstancePropertiesBuilder()
                .sts(sts).regionProvider(() -> Region.of(localStackContainer.getRegion()))
                .build().generate();

        // When
        SaveLocalProperties.saveToDirectory(tempDir, properties, Stream.empty());

        // Then
        assertThat(tempDir.resolve("configBucket.txt")).exists();
        assertThat(tempDir.resolve("queryResultsBucket.txt")).exists();
    }

    @Test
    void shouldSaveBucketNamesToLocalDirectoryWhenTablePropertiesGenerated() throws IOException {
        // Given
        InstanceProperties instanceProperties = generateInstancePropertiesBuilder()
                .sts(sts).regionProvider(() -> Region.of(localStackContainer.getRegion()))
                .build().generate();
        TableProperties tableProperties = GenerateTableProperties.from(instanceProperties, schemaWithKey("key"), "test-table");

        // When
        SaveLocalProperties.saveToDirectory(tempDir, instanceProperties, Stream.of(tableProperties));

        // Then
        assertThat(tempDir.resolve("tables/test-table/tableBucket.txt")).exists();
    }

    private GenerateInstanceProperties.Builder generateInstancePropertiesBuilder() {
        return GenerateInstanceProperties.builder()
                .instanceId("test-instance").vpcId("some-vpc").subnetId("some-subnet");
    }
}
