/*
 * Copyright 2022-2026 Crown Copyright
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
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;

import sleeper.clients.util.console.ConsoleInput;
import sleeper.core.util.FilesUtil;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandLineUsage;
import sleeper.core.util.cli.CommandOption;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

import static sleeper.core.properties.instance.CommonProperty.ID;

/**
 * Deletes a Sleeper instance and associated artefacts.
 */
public class TearDownInstance {
    private static final Logger LOGGER = LoggerFactory.getLogger(TearDownInstance.class);

    private final CloudFormationClient cloudFormationClient;
    private final Path scriptsDir;
    private final String instanceId;

    private TearDownInstance(Builder builder) {
        cloudFormationClient = Objects.requireNonNull(builder.cloudFormationClient, "cloudFormationClient must not be null");
        scriptsDir = Objects.requireNonNull(builder.scriptsDir, "scriptsDir must not be null");
        instanceId = Objects.requireNonNull(builder.instanceId, "instanceId must not be null");
    }

    public static final CommandLineUsage USAGE = CommandLineUsage.builder()
            .positionalArguments(List.of("scripts directory", "instance ID"))
            .systemArguments(List.of("scripts directory"))
            .options(List.of(CommandOption.longFlag("force")))
            .helpSummary("" +
                    "Deletes a Sleeper instance and associated artefacts.\n" +
                    "\n" +
                    "--force\n" +
                    "Skips the confirmation prompt before deleting the instance.")
            .build();

    public static void main(String[] rawArgs) throws IOException, InterruptedException {
        Arguments args = CommandArguments.parseAndValidateOrExit(USAGE, rawArgs, TearDownInstance::readArguments);
        if (!confirmTearDown(ConsoleInput.stdIn(), args)) {
            return;
        }
        builder().scriptsDir(args.scriptsDirectory())
                .instanceId(args.instanceId())
                .tearDownWithDefaultClients();
    }

    static Arguments readArguments(CommandArguments arguments) {
        return new Arguments(
                Path.of(arguments.getString("scripts directory")),
                arguments.getString("instance ID"),
                arguments.isFlagSet("force"));
    }

    static boolean confirmTearDown(ConsoleInput input, Arguments args) {
        if (args.force()) {
            return true;
        }
        String result = input.promptLine("Are you sure you want to tear down Sleeper instance " + args.instanceId() + "? [y/N]");
        return "y".equalsIgnoreCase(result);
    }

    /**
     * Command line arguments for tearing down an instance.
     *
     * @param scriptsDirectory the Sleeper scripts directory
     * @param instanceId       the instance ID to tear down
     * @param force            whether to skip the confirmation prompt
     */
    record Arguments(Path scriptsDirectory, String instanceId, boolean force) {
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Deletes the Sleeper instance and artefacts.
     *
     * @throws IOException          if the local copy of configuration files could not be deleted
     * @throws InterruptedException if the process was interrupted while waiting for a stack to be deleted
     */
    public void tearDown() throws IOException, InterruptedException {
        LOGGER.info("--------------------------------------------------------");
        LOGGER.info("Tear Down");
        LOGGER.info("--------------------------------------------------------");
        LOGGER.info("scriptsDir: {}", scriptsDir);
        LOGGER.info("{}: {}", ID.getPropertyName(), instanceId);

        deleteStack();
        waitForStackToDelete();
        deleteArtefactsStack();
        waitForArtefactsStackToDelete();
        removeGeneratedDir(scriptsDir);

        LOGGER.info("Finished tear down");
    }

    /**
     * Deletes the Sleeper instance CloudFormation stack. This will return after the request is submitted to
     * CloudFormation. Use {@link #waitForStackToDelete()} to wait for it to finish.
     */
    public void deleteStack() {
        deleteStack(instanceId);
    }

    /**
     * Deletes the artefacts CloudFormation stack. This should be called after the instance has been deleted. This will
     * return after the request is submitted to CloudFormation. Use {@link #waitForArtefactsStackToDelete()} to wait for
     * it to finish.
     */
    public void deleteArtefactsStack() {
        deleteStack(artefactsStackName());
    }

    private void deleteStack(String stackName) {
        LOGGER.info("Deleting instance CloudFormation stack: {}", stackName);
        try {
            cloudFormationClient.deleteStack(builder -> builder.stackName(stackName));
        } catch (RuntimeException e) {
            LOGGER.warn("Failed deleting stack", e);
        }
    }

    /**
     * Waits for the Sleeper instance CloudFormation stack to be deleted. Should be called after {@link #deleteStack()}.
     *
     * @throws InterruptedException if the process was interrupted while waiting for the stack to be deleted
     */
    public void waitForStackToDelete() throws InterruptedException {
        WaitForStackToDelete.from(cloudFormationClient, instanceId).pollUntilFinished();
    }

    /**
     * Waits for the artefacts CloudFormation stack to be deleted. Should be called after
     * {@link #deleteArtefactsStack()}.
     *
     * @throws InterruptedException if the process was interrupted while waiting for the stack to be deleted
     */
    public void waitForArtefactsStackToDelete() throws InterruptedException {
        WaitForStackToDelete.from(cloudFormationClient, artefactsStackName()).pollUntilFinished();
    }

    private String artefactsStackName() {
        return instanceId + "-artefacts";
    }

    private static void removeGeneratedDir(Path scriptsDir) throws IOException {
        Path generatedDir = scriptsDir.resolve("generated");
        if (Files.isDirectory(generatedDir)) {
            LOGGER.info("Removing generated files");
            FilesUtil.clearDirectory(generatedDir);
        } else {
            LOGGER.info("Generated directory not found");
        }
    }

    /**
     * Creates instances of this class.
     */
    public static final class Builder {
        private CloudFormationClient cloudFormationClient;
        private Path scriptsDir;
        private String instanceId;

        private Builder() {
        }

        /**
         * Sets the CloudFormation client to find and delete the stacks.
         *
         * @param  cloudFormationClient the CloudFormation client
         * @return                      this builder
         */
        public Builder cloudFormationClient(CloudFormationClient cloudFormationClient) {
            this.cloudFormationClient = cloudFormationClient;
            return this;
        }

        /**
         * Sets the local scripts directory. The generated configuration directory will be cleared when the tear down is
         * complete.
         *
         * @param  scriptsDir the scripts directory
         * @return            this builder
         */
        public Builder scriptsDir(Path scriptsDir) {
            this.scriptsDir = scriptsDir;
            return this;
        }

        /**
         * Sets the ID of the Sleeper instance to delete.
         *
         * @param  instanceId the instance ID
         * @return            this builder
         */
        public Builder instanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public TearDownInstance build() {
            return new TearDownInstance(this);
        }

        /**
         * Creates a CloudFormation client and runs the tear down.
         *
         * @throws IOException          if the local copy of configuration files could not be deleted
         * @throws InterruptedException if the process was interrupted while waiting for a stack to be deleted
         */
        public void tearDownWithDefaultClients() throws IOException, InterruptedException {
            try (CloudFormationClient cloudFormationClient = CloudFormationClient.create()) {
                cloudFormationClient(cloudFormationClient).build().tearDown();
            }
        }
    }
}
