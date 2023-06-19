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

import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.providers.AwsRegionProvider;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.util.ClientUtils;
import sleeper.clients.util.cdk.CdkCommand;
import sleeper.clients.util.cdk.InvokeCdkForInstance;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.local.SaveLocalProperties;
import sleeper.configuration.properties.table.TableProperties;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static sleeper.clients.util.ClientUtils.optionalArgument;
import static sleeper.configuration.properties.PropertiesUtils.loadProperties;
import static sleeper.configuration.properties.table.TableProperty.SPLIT_POINTS_FILE;

public class DeployNewInstance {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeployNewInstance.class);

    private final AWSSecurityTokenService sts;
    private final AwsRegionProvider regionProvider;
    private final S3Client s3;
    private final Path scriptsDirectory;
    private final String instanceId;
    private final String vpcId;
    private final String subnetId;
    private final String tableName;
    private final Path instanceProperties;
    private final Consumer<InstanceProperties> extraInstanceProperties;
    private final InvokeCdkForInstance.Type instanceType;
    private final Path splitPointsFile;
    private final boolean deployPaused;

    private DeployNewInstance(Builder builder) {
        sts = builder.sts;
        regionProvider = builder.regionProvider;
        s3 = builder.s3;
        scriptsDirectory = builder.scriptsDirectory;
        instanceId = builder.instanceId;
        vpcId = builder.vpcId;
        subnetId = builder.subnetId;
        tableName = builder.tableName;
        instanceProperties = builder.instanceProperties;
        extraInstanceProperties = builder.extraInstanceProperties;
        instanceType = builder.instanceType;
        splitPointsFile = builder.splitPointsFile;
        deployPaused = builder.deployPaused;
        if (splitPointsFile != null && !Files.exists(splitPointsFile)) {
            throw new IllegalArgumentException("Split points file not found: " + splitPointsFile);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 5 || args.length > 7) {
            throw new IllegalArgumentException("Usage: <scripts-dir> <instance-id> <vpc> <subnet> <table-name> " +
                    "<optional-deploy-paused-flag> <optional-split-points-file>");
        }
        Path scriptsDirectory = Path.of(args[0]);

        builder().scriptsDirectory(scriptsDirectory)
                .instanceId(args[1])
                .vpcId(args[2])
                .subnetId(args[3])
                .tableName(args[4])
                .deployPaused("true".equalsIgnoreCase(optionalArgument(args, 5).orElse("false")))
                .splitPointsFile(optionalArgument(args, 6).map(Path::of).orElse(null))
                .instancePropertiesTemplate(scriptsDirectory.resolve("templates/instanceproperties.template"))
                .instanceType(InvokeCdkForInstance.Type.STANDARD)
                .deployWithDefaultClients();
    }

    public void deploy() throws IOException, InterruptedException {
        LOGGER.info("-------------------------------------------------------");
        LOGGER.info("Running Deployment");
        LOGGER.info("-------------------------------------------------------");

        Path templatesDirectory = scriptsDirectory.resolve("templates");
        Path generatedDirectory = scriptsDirectory.resolve("generated");
        Path jarsDirectory = scriptsDirectory.resolve("jars");
        String sleeperVersion = Files.readString(templatesDirectory.resolve("version.txt"));

        LOGGER.info("instanceId: {}", instanceId);
        LOGGER.info("vpcId: {}", vpcId);
        LOGGER.info("subnetId: {}", subnetId);
        LOGGER.info("tableName: {}", tableName);
        LOGGER.info("templatesDirectory: {}", templatesDirectory);
        LOGGER.info("generatedDirectory: {}", generatedDirectory);
        LOGGER.info("instancePropertiesTemplate: {}", instanceProperties);
        LOGGER.info("scriptsDirectory: {}", scriptsDirectory);
        LOGGER.info("jarsDirectory: {}", jarsDirectory);
        LOGGER.info("sleeperVersion: {}", sleeperVersion);
        LOGGER.info("splitPointsFile: {}", splitPointsFile);
        LOGGER.info("deployPaused: {}", deployPaused);

        Properties tagsProperties = loadProperties(templatesDirectory.resolve("tags.template"));
        tagsProperties.setProperty("InstanceID", instanceId);
        InstanceProperties instanceProperties = PopulateInstanceProperties.builder()
                .sts(sts).regionProvider(regionProvider)
                .instanceProperties(this.instanceProperties)
                .tagsProperties(tagsProperties)
                .instanceId(instanceId).vpcId(vpcId).subnetId(subnetId)
                .build().populate();
        extraInstanceProperties.accept(instanceProperties);
        TableProperties tableProperties = PopulateTableProperties.from(instanceProperties,
                Files.readString(templatesDirectory.resolve("schema.template")),
                templatesDirectory.resolve("tableproperties.template"),
                tableName);
        tableProperties.set(SPLIT_POINTS_FILE, Objects.toString(splitPointsFile, null));
        boolean jarsChanged = SyncJars.builder().s3(s3)
                .jarsDirectory(jarsDirectory).instanceProperties(instanceProperties)
                .deleteOldJars(false).build().sync();
        UploadDockerImages.builder()
                .baseDockerDirectory(scriptsDirectory.resolve("docker"))
                .uploadDockerImagesScript(scriptsDirectory.resolve("deploy/uploadDockerImages.sh"))
                .skipIf(!jarsChanged)
                .instanceProperties(instanceProperties)
                .build().upload();

        Files.createDirectories(generatedDirectory);
        ClientUtils.clearDirectory(generatedDirectory);
        SaveLocalProperties.saveToDirectory(generatedDirectory, instanceProperties, Stream.of(tableProperties));

        LOGGER.info("-------------------------------------------------------");
        LOGGER.info("Deploying Stacks");
        LOGGER.info("-------------------------------------------------------");
        CdkCommand cdkCommand = deployPaused ? CdkCommand.deployNewPaused() : CdkCommand.deployNew();
        InvokeCdkForInstance.builder()
                .instancePropertiesFile(generatedDirectory.resolve("instance.properties"))
                .jarsDirectory(jarsDirectory).version(sleeperVersion)
                .build().invoke(instanceType, cdkCommand);
        LOGGER.info("Finished deployment of new instance");
    }

    public static final class Builder {
        private AWSSecurityTokenService sts;
        private AwsRegionProvider regionProvider;
        private S3Client s3;
        private Path scriptsDirectory;
        private String instanceId;
        private String vpcId;
        private String subnetId;
        private String tableName;
        private Path instanceProperties;
        private Consumer<InstanceProperties> extraInstanceProperties = properties -> {
        };
        private InvokeCdkForInstance.Type instanceType;
        private Path splitPointsFile;
        private boolean deployPaused;

        private Builder() {
        }

        public Builder sts(AWSSecurityTokenService sts) {
            this.sts = sts;
            return this;
        }

        public Builder regionProvider(AwsRegionProvider regionProvider) {
            this.regionProvider = regionProvider;
            return this;
        }

        public Builder s3(S3Client s3) {
            this.s3 = s3;
            return this;
        }

        public Builder scriptsDirectory(Path scriptsDirectory) {
            this.scriptsDirectory = scriptsDirectory;
            return this;
        }

        public Builder instanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder vpcId(String vpcId) {
            this.vpcId = vpcId;
            return this;
        }

        public Builder subnetId(String subnetId) {
            this.subnetId = subnetId;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder instancePropertiesTemplate(Path instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        public Builder extraInstanceProperties(Consumer<InstanceProperties> extraInstanceProperties) {
            this.extraInstanceProperties = extraInstanceProperties;
            return this;
        }

        public Builder instanceType(InvokeCdkForInstance.Type instanceType) {
            this.instanceType = instanceType;
            return this;
        }

        public Builder splitPointsFile(Path splitPointsFile) {
            this.splitPointsFile = splitPointsFile;
            return this;
        }

        public Builder deployPaused(boolean deployPaused) {
            this.deployPaused = deployPaused;
            return this;
        }

        public DeployNewInstance build() {
            return new DeployNewInstance(this);
        }

        public void deployWithDefaultClients() throws IOException, InterruptedException {

            try (S3Client s3Client = S3Client.create()) {
                sts(AWSSecurityTokenServiceClientBuilder.defaultClient());
                regionProvider(DefaultAwsRegionProviderChain.builder().build());
                s3(s3Client);
                build().deploy();
            }
        }
    }
}
