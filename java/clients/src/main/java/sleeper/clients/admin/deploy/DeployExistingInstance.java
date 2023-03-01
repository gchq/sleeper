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

import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.local.SaveLocalProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.SleeperVersion;
import sleeper.util.ClientUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class DeployExistingInstance {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeployExistingInstance.class);
    private final Path scriptsDirectory;
    private final String instanceId;
    private final AmazonS3 s3;
    private final S3Client s3v2;
    private final AmazonECS ecs;
    private final LambdaClient lambda;

    private DeployExistingInstance(Builder builder) {
        scriptsDirectory = builder.scriptsDirectory;
        instanceId = builder.instanceId;
        s3 = builder.s3;
        s3v2 = builder.s3v2;
        ecs = builder.ecs;
        lambda = builder.lambda;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (2 != args.length) {
            throw new IllegalArgumentException("Usage: <scripts-dir> <instance-id>");
        }
        Path scriptsDirectory = Path.of(args[0]);

        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        AmazonECS ecs = AmazonECSClientBuilder.defaultClient();
        try (S3Client s3v2 = S3Client.create();
             LambdaClient lambda = LambdaClient.create()) {
            builder().s3(s3).s3v2(s3v2).ecs(ecs)
                    .lambda(lambda)
                    .instanceId(args[1])
                    .scriptsDirectory(scriptsDirectory)
                    .build().update();
        }
    }

    public void update() throws IOException, InterruptedException {
        LOGGER.info("-------------------------------------------------------");
        LOGGER.info("Running Deployment");
        LOGGER.info("-------------------------------------------------------");
        // Get instance properties from s3
        InstanceProperties properties = new InstanceProperties();
        properties.loadFromS3GivenInstanceId(s3, instanceId);
        Path generatedDirectory = scriptsDirectory.resolve("generated");
        Path jarsDirectory = scriptsDirectory.resolve("jars");
        Files.createDirectories(generatedDirectory);
        ClientUtils.clearDirectory(generatedDirectory);
        SaveLocalProperties.saveToDirectory(generatedDirectory, properties, TableProperties.streamTablesFromS3(s3, properties));

        boolean jarsChanged = SyncJars.builder().s3(s3v2)
                .jarsDirectory(jarsDirectory).instanceProperties(properties)
                .deleteOldJars(false)
                .build().sync();

        UploadDockerImages.builder()
                .baseDockerDirectory(scriptsDirectory.resolve("docker"))
                .uploadDockerImagesScript(scriptsDirectory.resolve("deploy/uploadDockerImages.sh"))
                .skipIf(!jarsChanged)
                .instanceProperties(properties)
                .build().upload();

        LOGGER.info("-------------------------------------------------------");
        LOGGER.info("Deploying Stacks");
        LOGGER.info("-------------------------------------------------------");
        CdkDeployInstance.builder()
                .instancePropertiesFile(generatedDirectory.resolve("instance.properties"))
                .version(SleeperVersion.getVersion())
                .jarsDirectory(jarsDirectory)
                .ensureNewInstance(false).skipVersionCheck(true)
                .build().deployInferringType(properties);

        // Update system-defined properties set by CDK
        properties = SaveLocalProperties.saveFromS3(s3, instanceId, generatedDirectory);

        RestartTasks.builder().ecs(ecs)
                .lambda(lambda)
                .properties(properties)
                .skipIf(!jarsChanged)
                .build().run();
        LOGGER.info("Finished deployment of existing instance");
    }

    public static final class Builder {
        private Path scriptsDirectory;
        private String instanceId;
        private AmazonS3 s3;
        private S3Client s3v2;
        private AmazonECS ecs;
        private LambdaClient lambda;

        private Builder() {
        }

        public Builder scriptsDirectory(Path scriptsDirectory) {
            this.scriptsDirectory = scriptsDirectory;
            return this;
        }

        public Builder instanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder s3(AmazonS3 s3) {
            this.s3 = s3;
            return this;
        }

        public Builder s3v2(S3Client s3v2) {
            this.s3v2 = s3v2;
            return this;
        }

        public Builder ecs(AmazonECS ecs) {
            this.ecs = ecs;
            return this;
        }

        public Builder lambda(LambdaClient lambda) {
            this.lambda = lambda;
            return this;
        }

        public DeployExistingInstance build() {
            return new DeployExistingInstance(this);
        }
    }
}
