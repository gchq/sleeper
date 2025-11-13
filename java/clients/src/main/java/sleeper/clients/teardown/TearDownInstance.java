/*
 * Copyright 2022-2025 Crown Copyright
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.deploy.AwsScheduleRules;
import sleeper.clients.util.ClientUtils;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.local.LoadLocalProperties;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

import static sleeper.clients.util.ClientUtils.optionalArgument;
import static sleeper.core.properties.instance.CommonProperty.ID;

public class TearDownInstance {
    private static final Logger LOGGER = LoggerFactory.getLogger(TearDownInstance.class);

    private final TearDownClients clients;
    private final Path scriptsDir;
    private final String instanceId;

    private TearDownInstance(Builder builder) {
        clients = Objects.requireNonNull(builder.clients, "clients must not be null");
        scriptsDir = Objects.requireNonNull(builder.scriptsDir, "scriptsDir must not be null");
        instanceId = Optional.ofNullable(builder.instanceId)
                .orElseGet(() -> loadInstanceIdFromGeneratedDirectory(scriptsDir));
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
        LOGGER.info("{}: {}", ID.getPropertyName(), instanceId);

        shutdownSystemProcesses();
        deleteStack();
        waitForStackToDelete();
        deleteArtefactsStack();
        waitForArtefactsStackToDelete();
        removeGeneratedDir(scriptsDir);

        LOGGER.info("Finished tear down");
    }

    public void shutdownSystemProcesses() throws InterruptedException {
        new AwsScheduleRules(clients.getCloudWatch()).pauseInstance(instanceId);
    }

    public void deleteStack() {
        deleteStack(instanceId);
    }

    public void deleteArtefactsStack() throws InterruptedException {
        deleteStack(artefactsStackName());
    }

    private void deleteStack(String stackName) {
        LOGGER.info("Deleting instance CloudFormation stack: {}", stackName);
        try {
            clients.getCloudFormation().deleteStack(builder -> builder.stackName(stackName));
        } catch (RuntimeException e) {
            LOGGER.warn("Failed deleting stack", e);
        }
    }

    public void waitForStackToDelete() throws InterruptedException {
        WaitForStackToDelete.from(clients.getCloudFormation(), instanceId).pollUntilFinished();
    }

    public void waitForArtefactsStackToDelete() throws InterruptedException {
        WaitForStackToDelete.from(clients.getCloudFormation(), artefactsStackName()).pollUntilFinished();
    }

    private String artefactsStackName() {
        return instanceId + "-artefacts";
    }

    public static void removeGeneratedDir(Path scriptsDir) throws IOException {
        Path generatedDir = scriptsDir.resolve("generated");
        if (Files.isDirectory(generatedDir)) {
            LOGGER.info("Removing generated files");
            ClientUtils.clearDirectory(generatedDir);
        } else {
            LOGGER.info("Generated directory not found");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private static String loadInstanceIdFromGeneratedDirectory(Path scriptsDir) {
        InstanceProperties instanceProperties = LoadLocalProperties
                .loadInstancePropertiesNoValidationFromDirectory(scriptsDir.resolve("generated"));
        return instanceProperties.get(ID);
    }

    public static final class Builder {
        private TearDownClients clients;
        private Path scriptsDir;
        private String instanceId;

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

        public TearDownInstance build() {
            return new TearDownInstance(this);
        }

        public void tearDownWithDefaultClients() throws IOException, InterruptedException {
            TearDownClients.withDefaults(clients -> clients(clients).build().tearDown());
        }
    }
}
