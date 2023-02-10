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

package sleeper.clients.admin;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.InstancePropertiesTestHelper;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesTestHelper;

import java.nio.file.Path;

import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class AdminCdkDeployTest {
    @TempDir
    private Path tempDir;

    @Test
    @Disabled("TODO")
    void shouldDeployFreshInstanceUsingCdk() {
        // Given
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        InstanceProperties instanceProperties = InstancePropertiesTestHelper.createTestInstanceProperties(s3Client);
        TableProperties tableProperties = TablePropertiesTestHelper.createTestTableProperties(
                instanceProperties, schemaWithKey("key"));

        // When we deploy cdk
        AdminCdkDeploy adminCdkDeploy = AdminCdkDeploy.withLocalConfigDir(tempDir);

        // Then the statestore has only one partition in it

    }
}
