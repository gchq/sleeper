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
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.local.SaveLocalProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesTestHelper;
import sleeper.util.ClientUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.stream.Stream;

import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class SystemTestInstance implements BeforeAllCallback {

    private final String instanceId = System.getProperty("sleeper.system.test.instance.id");
    private final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
    private final AWSSecurityTokenService sts = AWSSecurityTokenServiceClientBuilder.defaultClient();
    private final Path scriptsDir = findScriptsDir();
    private final Path generatedDir = scriptsDir.resolve("generated");
    private final Path jarsDir = scriptsDir.resolve("jars");
    private final String sleeperVersion = System.getProperty("sleeper.system.test.version");
    private final String vpcId = System.getProperty("sleeper.system.test.vpc.id");
    private final String subnetId = System.getProperty("sleeper.system.test.subnet.id");

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        InstanceProperties instanceProperties = GenerateInstanceProperties.builder()
                .s3(s3Client).sts(sts)
                .instanceId(instanceId)
                .sleeperVersion(sleeperVersion)
                .vpcId(vpcId).subnetId(subnetId)
                .build().generate();
        TableProperties tableProperties = TablePropertiesTestHelper.createTestTableProperties(
                instanceProperties, schemaWithKey("key"), "single-key");
        ClientUtils.clearDirectory(generatedDir);
        SaveLocalProperties.saveToDirectory(generatedDir, instanceProperties, Stream.of(tableProperties));
        PreDeployInstance.builder()
                .s3(s3Client)
                .jarsDirectory(jarsDir)
                .instanceProperties(instanceProperties)
                .build().preDeploy();
    }

    public InstanceProperties loadInstanceProperties() {
        InstanceProperties instanceProperties = new InstanceProperties();
        try {
            instanceProperties.loadFromS3GivenInstanceId(s3Client, instanceId);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return instanceProperties;
    }

    public AmazonS3 getS3Client() {
        return s3Client;
    }

    private static Path findScriptsDir() {
        return findJavaDir().getParent().resolve("scripts");
    }

    private static Path findJavaDir() {
        return findJavaDir(Path.of(".").toAbsolutePath());
    }

    private static Path findJavaDir(Path currentPath) {
        if ("java".equals(currentPath.getFileName().toString())) {
            return currentPath;
        } else {
            return findJavaDir(currentPath.getParent());
        }
    }
}
