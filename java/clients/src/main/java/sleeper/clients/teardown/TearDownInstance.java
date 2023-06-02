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
package sleeper.clients.teardown;

import com.amazonaws.services.cloudwatchevents.AmazonCloudWatchEvents;
import com.amazonaws.services.cloudwatchevents.AmazonCloudWatchEventsClientBuilder;
import com.amazonaws.services.ecr.AmazonECR;
import com.amazonaws.services.ecr.AmazonECRClientBuilder;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

import sleeper.clients.deploy.GenerateInstanceProperties;
import sleeper.clients.status.update.DownloadConfig;
import sleeper.clients.util.ClientUtils;
import sleeper.clients.util.cdk.CdkCommand;
import sleeper.clients.util.cdk.InvokeCdkForInstance;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.InstanceProperty;
import sleeper.configuration.properties.local.LoadLocalProperties;
import sleeper.core.SleeperVersion;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static sleeper.clients.util.ClientUtils.optionalArgument;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;

public class TearDownInstance {
    private static final Logger LOGGER = LoggerFactory.getLogger(TearDownInstance.class);

    private final AmazonS3 s3;
    private final S3Client s3v2;
    private final AmazonCloudWatchEvents cloudWatch;
    private final AmazonECS ecs;
    private final AmazonECR ecr;
    private final AmazonElasticMapReduce emr;
    private final Path scriptsDir;
    private final Path generatedDir;
    private final String instanceIdArg;
    private final Function<InstanceProperties, List<String>> getExtraEcsClusters;
    private final List<InstanceProperty> extraEcrRepositories;

    private TearDownInstance(Builder builder) {
        s3 = Objects.requireNonNull(builder.s3, "s3 must not be null");
        s3v2 = Objects.requireNonNull(builder.s3v2, "s3v2 must not be null");
        cloudWatch = Objects.requireNonNull(builder.cloudWatch, "cloudWatch must not be null");
        ecs = Objects.requireNonNull(builder.ecs, "ecs must not be null");
        ecr = Objects.requireNonNull(builder.ecr, "ecr must not be null");
        emr = Objects.requireNonNull(builder.emr, "emr must not be null");
        scriptsDir = Objects.requireNonNull(builder.scriptsDir, "scriptsDir must not be null");
        getExtraEcsClusters = Objects.requireNonNull(builder.getExtraEcsClusters, "getExtraEcsClusters must not be null");
        extraEcrRepositories = Objects.requireNonNull(builder.extraEcrRepositories, "extraEcrRepositories must not be null");
        instanceIdArg = builder.instanceId;
        generatedDir = scriptsDir.resolve("generated");
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 1 || args.length > 2) {
            throw new IllegalArgumentException("Usage: <scripts directory> <optional instance id>");
        }
        builder().scriptsDir(Path.of(args[0]))
                .instanceId(optionalArgument(args, 1).orElse(null))
                .tearDownWithDefaultClients();
    }

    public void tearDown() throws IOException, InterruptedException {
        InstanceProperties instanceProperties = loadInstanceConfig();

        LOGGER.info("--------------------------------------------------------");
        LOGGER.info("Tear Down");
        LOGGER.info("--------------------------------------------------------");
        LOGGER.info("scriptsDir: {}", scriptsDir);
        LOGGER.info("generatedDir: {}", generatedDir);
        LOGGER.info("{}: {}", ID.getPropertyName(), instanceProperties.get(ID));
        LOGGER.info("{}: {}", CONFIG_BUCKET.getPropertyName(), instanceProperties.get(CONFIG_BUCKET));
        LOGGER.info("{}: {}", QUERY_RESULTS_BUCKET.getPropertyName(), instanceProperties.get(QUERY_RESULTS_BUCKET));

        new ShutdownSystemProcesses(cloudWatch, ecs, emr)
                .shutdown(instanceProperties, getExtraEcsClusters.apply(instanceProperties));

        LOGGER.info("Running cdk destroy to remove the system");
        InvokeCdkForInstance.builder()
                .instancePropertiesFile(generatedDir.resolve("instance.properties"))
                .jarsDirectory(scriptsDir.resolve("jars"))
                .version(SleeperVersion.getVersion()).build()
                .invokeInferringType(instanceProperties, CdkCommand.destroy());

        LOGGER.info("Removing the Jars bucket and docker containers");
        RemoveJarsBucket.remove(s3v2, instanceProperties.get(JARS_BUCKET));
        RemoveECRRepositories.remove(ecr, instanceProperties, extraEcrRepositories);

        LOGGER.info("Removing generated files");
        ClientUtils.clearDirectory(generatedDir);

        LOGGER.info("Successfully torn down");
    }

    public static Builder builder() {
        return new Builder();
    }

    private InstanceProperties loadInstanceConfig() throws IOException {
        String instanceId;
        if (instanceIdArg == null) {
            InstanceProperties instanceProperties = LoadLocalProperties.loadInstancePropertiesFromDirectory(generatedDir);
            instanceId = instanceProperties.get(ID);
        } else {
            instanceId = instanceIdArg;
        }
        LOGGER.info("Updating configuration for instance {}", instanceId);
        try {
            return DownloadConfig.overwriteTargetDirectoryIfDownloadSuccessful(
                    s3, instanceId, generatedDir, Path.of("/tmp/sleeper/generated"));
        } catch (NoSuchBucketException e) {
            LOGGER.info("Failed to update configuration, using default properties");
            return GenerateInstanceProperties.generateDefaultsFromInstanceId(instanceId);
        }
    }

    public static final class Builder {
        private AmazonS3 s3;
        private S3Client s3v2;
        private AmazonCloudWatchEvents cloudWatch;
        private AmazonECS ecs;
        private AmazonECR ecr;
        private AmazonElasticMapReduce emr;
        private Path scriptsDir;
        private String instanceId;
        private Function<InstanceProperties, List<String>> getExtraEcsClusters = properties -> List.of();
        private List<InstanceProperty> extraEcrRepositories = List.of();

        private Builder() {
        }

        public Builder s3(AmazonS3 s3) {
            this.s3 = s3;
            return this;
        }

        public Builder s3v2(S3Client s3v2) {
            this.s3v2 = s3v2;
            return this;
        }

        public Builder cloudWatch(AmazonCloudWatchEvents cloudWatch) {
            this.cloudWatch = cloudWatch;
            return this;
        }

        public Builder ecs(AmazonECS ecs) {
            this.ecs = ecs;
            return this;
        }

        public Builder ecr(AmazonECR ecr) {
            this.ecr = ecr;
            return this;
        }

        public Builder emr(AmazonElasticMapReduce emr) {
            this.emr = emr;
            return this;
        }

        public Builder scriptsDir(Path scriptsDir) {
            this.scriptsDir = scriptsDir;
            return this;
        }

        public Builder instanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder getExtraEcsClusters(Function<InstanceProperties, List<String>> getExtraEcsClusters) {
            this.getExtraEcsClusters = getExtraEcsClusters;
            return this;
        }

        public Builder extraEcrRepositories(List<InstanceProperty> extraEcrRepositories) {
            this.extraEcrRepositories = extraEcrRepositories;
            return this;
        }

        public TearDownInstance build() {
            return new TearDownInstance(this);
        }

        public void tearDownWithDefaultClients() throws IOException, InterruptedException {
            try (S3Client s3v2Client = S3Client.create()) {
                s3(AmazonS3ClientBuilder.defaultClient());
                s3v2(s3v2Client);
                cloudWatch(AmazonCloudWatchEventsClientBuilder.defaultClient());
                ecs(AmazonECSClientBuilder.defaultClient());
                ecr(AmazonECRClientBuilder.defaultClient());
                emr(AmazonElasticMapReduceClientBuilder.defaultClient());
                build().tearDown();
            }
        }
    }
}
