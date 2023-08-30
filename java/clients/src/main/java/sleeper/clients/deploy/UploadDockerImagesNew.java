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

import sleeper.clients.util.EcrRepositories;
import sleeper.clients.util.RunCommandPipeline;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.SleeperVersion;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static sleeper.clients.util.Command.command;
import static sleeper.clients.util.CommandPipeline.pipeline;
import static sleeper.configuration.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.configuration.properties.instance.CommonProperty.REGION;

public class UploadDockerImagesNew {
    private static final Map<String, String> DIRECTORY_BY_STACK = Map.of(
            "IngestStack", "ingest",
            "EksBulkImportStack", "bulk-import-runner"
    );

    private final Path baseDockerDirectory;
    private final String id;
    private final String account;
    private final String region;
    private final String version;
    private final List<String> stacks;
    private final EcrRepositories.Client ecrClient;

    private UploadDockerImagesNew(Builder builder) {
        baseDockerDirectory = requireNonNull(builder.baseDockerDirectory, "baseDockerDirectory must not be null");
        id = requireNonNull(builder.id, "id must not be null");
        account = requireNonNull(builder.account, "account must not be null");
        region = requireNonNull(builder.region, "region must not be null");
        version = requireNonNull(builder.version, "version must not be null");
        stacks = requireNonNull(builder.stacks, "stacks must not be null");
        ecrClient = requireNonNull(builder.ecrClient, "ecrClient must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public void upload(RunCommandPipeline runCommand) throws IOException, InterruptedException {
        String repositoryHost = String.format("%s.dkr.ecr.%s.amazonaws.com", account, region);

        List<String> dockerDirectories = stacks.stream()
                .filter(DIRECTORY_BY_STACK::containsKey)
                .map(DIRECTORY_BY_STACK::get)
                .filter(directory -> !ecrClient.repositoryExists(repositoryNameForDirectory(directory)))
                .collect(Collectors.toUnmodifiableList());

        if (!dockerDirectories.isEmpty()) {
            runCommand.run(pipeline(
                    command("aws", "ecr", "get-login-password", "--region", region),
                    command("docker", "login", "--username", "AWS", "--password-stdin", repositoryHost)));
        }

        for (String directory : dockerDirectories) {
            String repositoryName = repositoryNameForDirectory(directory);
            ecrClient.createRepository(repositoryName);

            String tag = repositoryHost + "/" + repositoryName + ":" + version;
            runCommand.run("docker", "build", "-t", tag,
                    baseDockerDirectory.resolve(directory).toString());
            runCommand.run("docker", "push", tag);
        }
    }

    private String repositoryNameForDirectory(String directory) {
        return id + "/" + directory;
    }

    public static final class Builder {
        private Path baseDockerDirectory;
        private String id;
        private String account;
        private String region;
        private String version = SleeperVersion.getVersion();
        private List<String> stacks;
        private EcrRepositories.Client ecrClient;

        private Builder() {
        }

        public Builder baseDockerDirectory(Path baseDockerDirectory) {
            this.baseDockerDirectory = baseDockerDirectory;
            return this;
        }

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            return id(instanceProperties.get(ID))
                    .account(instanceProperties.get(ACCOUNT))
                    .region(instanceProperties.get(REGION))
                    .stacks(instanceProperties.getList(OPTIONAL_STACKS));
        }

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder account(String account) {
            this.account = account;
            return this;
        }

        public Builder region(String region) {
            this.region = region;
            return this;
        }

        public Builder version(String version) {
            this.version = version;
            return this;
        }

        public Builder stacks(List<String> stacks) {
            this.stacks = stacks;
            return this;
        }

        public Builder ecrClient(EcrRepositories.Client ecrClient) {
            this.ecrClient = ecrClient;
            return this;
        }

        public UploadDockerImagesNew build() {
            return new UploadDockerImagesNew(this);
        }
    }
}
