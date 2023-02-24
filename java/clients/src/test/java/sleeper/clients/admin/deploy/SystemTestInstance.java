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

import com.amazonaws.services.ecr.AmazonECR;
import com.amazonaws.services.ecr.AmazonECRClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.local.SaveLocalProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.util.ClientUtils;

import java.nio.file.Path;
import java.util.stream.Stream;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class SystemTestInstance implements BeforeAllCallback {

    private final String instanceId = System.getProperty("sleeper.system.test.instance.id");
    private final AWSSecurityTokenService sts = AWSSecurityTokenServiceClientBuilder.defaultClient();
    private final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
    private final S3Client s3v2 = S3Client.create();
    private final AmazonECR ecr = AmazonECRClientBuilder.defaultClient();
    private final Path scriptsDir = findScriptsDir();
    private final Path jarsDir = scriptsDir.resolve("jars");
    private final Path dockerDir = scriptsDir.resolve("docker");
    private final Path generatedDir = scriptsDir.resolve("generated");
    private final String sleeperVersion = System.getProperty("sleeper.system.test.version");
    private final String vpcId = System.getProperty("sleeper.system.test.vpc.id");
    private final String subnetId = System.getProperty("sleeper.system.test.subnet.id");
    private InstanceProperties instanceProperties;
    private TableProperties singleKeyTableProperties;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        instanceProperties = GenerateInstanceProperties.builder()
                .sts(sts).regionProvider(DefaultAwsRegionProviderChain.builder().build())
                .instanceId(instanceId)
                .sleeperVersion(sleeperVersion)
                .vpcId(vpcId).subnetId(subnetId)
                .build().generate();
        singleKeyTableProperties = GenerateTableProperties.from(instanceProperties, schemaWithKey("key"), "single-key");
        PreDeployInstance.builder()
                .s3(s3v2).ecr(ecr)
                .jarsDirectory(jarsDir)
                .baseDockerDirectory(dockerDir)
                .uploadDockerImagesScript(scriptsDir.resolve("deploy/uploadDockerImages.sh"))
                .instanceProperties(instanceProperties)
                .reuploadDockerImages(false)
                .build().preDeploy();
        ClientUtils.clearDirectory(generatedDir);
        SaveLocalProperties.saveToDirectory(generatedDir, instanceProperties, Stream.of(singleKeyTableProperties));
        CdkDeployInstance.builder()
                .instancePropertiesFile(generatedDir.resolve("instance.properties"))
                .jarsDirectory(jarsDir).version(instanceProperties.get(VERSION))
                .ensureNewInstance(false)
                .build().deploy();
        instanceProperties.loadFromS3GivenInstanceId(s3Client, instanceProperties.get(ID));
        singleKeyTableProperties.loadFromS3(s3Client, singleKeyTableProperties.get(TABLE_NAME));
    }

    public InstanceProperties getInstanceProperties() {
        return instanceProperties;
    }

    public TableProperties getSingleKeyTableProperties() {
        return singleKeyTableProperties;
    }

    public AmazonS3 getS3Client() {
        return s3Client;
    }

    public AmazonECR getEcrClient() {
        return ecr;
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
