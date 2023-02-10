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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesTestHelper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;

import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

@Disabled
class AdminCdkDeployIT {
    private final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

    private final Path scriptsDir = Path.of(".").toAbsolutePath()
            .getParent().getParent().getParent().resolve("scripts");
    private final Path jarsDir = scriptsDir.resolve("jars");
    @TempDir
    private Path tempDir;

    @Test
    void shouldDeployFreshInstanceUsingCdk() throws IOException {
        // Given
        InstanceProperties instanceProperties = PreDeployNewInstance.builder()
                .s3(s3Client)
                .instanceId(UUID.randomUUID().toString())
                .jarsDirectory(jarsDir)
                .sleeperVersion("0.15.0-SNAPSHOT")
                .vpcId(System.getenv("VPC_ID"))
                .subnetId(System.getenv("SUBNET_ID"))
                .build().preDeploy();
        TablePropertiesTestHelper.createTestTableProperties(instanceProperties, schemaWithKey("key"), s3Client);
        AdminCdkDeploy.withLocalConfigDir(tempDir);

        // When we deploy cdk

        // Then the statestore has only one partition in it
    }
}
