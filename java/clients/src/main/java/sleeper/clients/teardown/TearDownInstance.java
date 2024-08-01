/*
 * Copyright 2022-2024 Crown Copyright
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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.deploy.PopulateInstanceProperties;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.local.LoadLocalProperties;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static sleeper.clients.util.ClientUtils.optionalArgument;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.ID;

public class TearDownInstance {
    private static final Logger LOGGER = LoggerFactory.getLogger(TearDownInstance.class);

    private final TearDownClients clients;
    private final Path scriptsDir;
    private final Function<InstanceProperties, List<String>> getExtraEcsClusters;
    private final Function<InstanceProperties, List<String>> getExtraEcrRepositories;
    private final InstanceProperties instanceProperties;

    private TearDownInstance(Builder builder) {
        clients = Objects.requireNonNull(builder.clients, "clients must not be null");
        scriptsDir = Objects.requireNonNull(builder.scriptsDir, "scriptsDir must not be null");
        getExtraEcsClusters = Objects.requireNonNull(builder.getExtraEcsClusters, "getExtraEcsClusters must not be null");
        getExtraEcrRepositories = Objects.requireNonNull(builder.getExtraEcrRepositories, "getExtraEcrRepositories must not be null");
        instanceProperties = builder.loadInstanceProperties();
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
        LOGGER.info("--------------------------------------------------------");
        LOGGER.info("Tear Down");
        LOGGER.info("--------------------------------------------------------");
        LOGGER.info("scriptsDir: {}", scriptsDir);
        LOGGER.info("{}: {}", ID.getPropertyName(), instanceProperties.get(ID));
        LOGGER.info("{}: {}", CONFIG_BUCKET.getPropertyName(), instanceProperties.get(CONFIG_BUCKET));
        LOGGER.info("{}: {}", QUERY_RESULTS_BUCKET.getPropertyName(), instanceProperties.get(QUERY_RESULTS_BUCKET));

        shutdownSystemProcesses();
        deleteStack();
        waitForStackToDelete();
        cleanupAfterStackDeletion();

        LOGGER.info("Finished tear down");
    }

    public void deleteStack() {
        String instanceId = instanceProperties.get(ID);
        LOGGER.info("Deleting instance CloudFormation stack: {}", instanceId);
        try {
            clients.getCloudFormation().deleteStack(builder -> builder.stackName(instanceId));
        } catch (RuntimeException e) {
            LOGGER.warn("Failed deleting stack", e);
        }
    }

    public void waitForStackToDelete() throws InterruptedException {
        WaitForStackToDelete.from(clients.getCloudFormation(), instanceProperties.get(ID)).pollUntilFinished();
    }

    public void shutdownSystemProcesses() throws InterruptedException {
        new ShutdownSystemProcesses(clients)
                .shutdown(instanceProperties, getExtraEcsClusters.apply(instanceProperties));
    }

    public void cleanupAfterStackDeletion() throws InterruptedException, IOException {
        new CleanupAfterStackDeletion(clients, scriptsDir).cleanup(instanceProperties, getExtraEcrRepositories.apply(instanceProperties));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static InstanceProperties loadInstancePropertiesOrGenerateDefaults(AmazonS3 s3, String instanceId) {
        LOGGER.info("Loading configuration for instance {}", instanceId);
        try {
            InstanceProperties properties = new InstanceProperties();
            properties.loadFromS3GivenInstanceId(s3, instanceId);
            return properties;
        } catch (AmazonS3Exception e) {
            LOGGER.info("Failed to download configuration, using default properties");
            return PopulateInstanceProperties.generateTearDownDefaultsFromInstanceId(instanceId);
        }
    }

    public static final class Builder {
        private TearDownClients clients;
        private Path scriptsDir;
        private String instanceId;
        private InstanceProperties instanceProperties;
        private Function<InstanceProperties, List<String>> getExtraEcsClusters = properties -> List.of();
        private Function<InstanceProperties, List<String>> getExtraEcrRepositories = properties -> List.of();

        private Builder() {
        }

        public Builder clients(TearDownClients clients) {
            this.clients = clients;
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

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        public Builder getExtraEcsClusters(Function<InstanceProperties, List<String>> getExtraEcsClusters) {
            this.getExtraEcsClusters = getExtraEcsClusters;
            return this;
        }

        public Builder getExtraEcrRepositories(Function<InstanceProperties, List<String>> getExtraEcrRepositories) {
            this.getExtraEcrRepositories = getExtraEcrRepositories;
            return this;
        }

        public TearDownInstance build() {
            return new TearDownInstance(this);
        }

        public void tearDownWithDefaultClients() throws IOException, InterruptedException {
            TearDownClients.withDefaults(clients -> clients(clients).build().tearDown());
        }

        private InstanceProperties loadInstanceProperties() {
            if (instanceProperties != null) {
                return instanceProperties;
            } else if (instanceId == null) {
                InstanceProperties instanceProperties = LoadLocalProperties.loadInstancePropertiesFromDirectory(scriptsDir.resolve("generated"));
                instanceId = instanceProperties.get(ID);
            }
            return loadInstancePropertiesOrGenerateDefaults(clients.getS3(), instanceId);
        }
    }
}
