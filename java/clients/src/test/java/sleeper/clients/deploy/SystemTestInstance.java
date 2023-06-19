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
package sleeper.clients.deploy;

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

import sleeper.clients.util.ClientUtils;
import sleeper.clients.util.cdk.CdkCommand;
import sleeper.clients.util.cdk.InvokeCdkForInstance;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.local.SaveLocalProperties;
import sleeper.configuration.properties.table.TableProperties;

import java.nio.file.Path;
import java.util.stream.Stream;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;
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
    private final String vpcId = System.getProperty("sleeper.system.test.vpc.id");
    private final String subnetId = System.getProperty("sleeper.system.test.subnet.id");
    private InstanceProperties instanceProperties;
    private TableProperties singleKeyTableProperties;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        instanceProperties = PopulateInstanceProperties.builder()
                .sts(sts).regionProvider(DefaultAwsRegionProviderChain.builder().build())
                .instanceId(instanceId)
                .vpcId(vpcId).subnetId(subnetId)
                .build().populate();
        singleKeyTableProperties = GenerateTableProperties.from(instanceProperties, schemaWithKey("key"), "single-key");
        boolean jarsChanged = SyncJars.builder().s3(s3v2)
                .jarsDirectory(jarsDir)
                .bucketName(instanceProperties.get(JARS_BUCKET))
                .region(instanceProperties.get(REGION))
                .deleteOldJars(true)
                .build().sync();
        UploadDockerImages.builder()
                .baseDockerDirectory(dockerDir)
                .uploadDockerImagesScript(scriptsDir.resolve("deploy/uploadDockerImages.sh"))
                .skipIf(!jarsChanged)
                .instanceProperties(instanceProperties)
                .build().upload();
        ClientUtils.clearDirectory(generatedDir);
        SaveLocalProperties.saveToDirectory(generatedDir, instanceProperties, Stream.of(singleKeyTableProperties));
        InvokeCdkForInstance.builder()
                .instancePropertiesFile(generatedDir.resolve("instance.properties"))
                .jarsDirectory(jarsDir).version(instanceProperties.get(VERSION))
                .build().invoke(InvokeCdkForInstance.Type.STANDARD, CdkCommand.deployExisting());
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
