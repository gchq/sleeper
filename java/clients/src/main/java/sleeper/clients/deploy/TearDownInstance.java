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

import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.local.LoadLocalProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.status.update.DownloadConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.util.ClientUtils.optionalArgument;

public class TearDownInstance {
    private static final Logger LOGGER = LoggerFactory.getLogger(TearDownInstance.class);

    private final AmazonS3 s3;
    private final Path scriptsDir;
    private final Path generatedDir;
    private final String instanceIdArg;
    private final List<String> extraEcsClusters;

    private TearDownInstance(Builder builder) {
        s3 = Objects.requireNonNull(builder.s3, "s3 must not be null");
        scriptsDir = Objects.requireNonNull(builder.scriptsDir, "scriptsDir must not be null");
        extraEcsClusters = Objects.requireNonNull(builder.extraEcsClusters, "extraEcsClusters must not be null");
        instanceIdArg = builder.instanceId;
        generatedDir = scriptsDir.resolve("generated");
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1 || args.length > 2) {
            throw new IllegalArgumentException("Usage: <scripts directory> <optional instance id>");
        }
        builder().scriptsDir(Path.of(args[0]))
                .instanceId(optionalArgument(args, 1).orElse(null))
                .tearDownWithDefaultClients();
    }

    public void tearDown() throws IOException {
        InstanceProperties instanceProperties = loadInstanceConfig();

        LOGGER.info("--------------------------------------------------------");
        LOGGER.info("Tear Down");
        LOGGER.info("--------------------------------------------------------");
        LOGGER.info("scriptsDir: {}", scriptsDir);
        LOGGER.info("generatedDir: {}", generatedDir);
        LOGGER.info("{}: {}", ID.getPropertyName(), instanceProperties.get(ID));
        LOGGER.info("{}: {}", CONFIG_BUCKET.getPropertyName(), instanceProperties.get(CONFIG_BUCKET));
        LOGGER.info("{}: {}", QUERY_RESULTS_BUCKET.getPropertyName(), instanceProperties.get(QUERY_RESULTS_BUCKET));

        List<TableProperties> tablePropertiesList = LoadLocalProperties
                .loadTablesFromDirectory(instanceProperties, scriptsDir).collect(Collectors.toList());
        AmazonECS ecs = AmazonECSClientBuilder.defaultClient();
        new CleanUpBeforeDestroy(s3, ecs).cleanUp(instanceProperties, tablePropertiesList, extraEcsClusters);
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
        return DownloadConfig.overwriteTargetDirectoryIfDownloadSuccessful(
                s3, instanceId, generatedDir, Path.of("/tmp/sleeper/generated"));
    }

    public static final class Builder {
        private AmazonS3 s3;
        private Path scriptsDir;
        private String instanceId;
        private List<String> extraEcsClusters = List.of();

        private Builder() {
        }

        public Builder s3(AmazonS3 s3) {
            this.s3 = s3;
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

        public Builder extraEcsClusters(List<String> extraEcsClusters) {
            this.extraEcsClusters = extraEcsClusters;
            return this;
        }

        public TearDownInstance build() {
            return new TearDownInstance(this);
        }

        public void tearDownWithDefaultClients() throws IOException {
            s3(AmazonS3ClientBuilder.defaultClient())
                    .build().tearDown();
        }
    }
}
