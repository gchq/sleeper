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
package sleeper.clients.deploy.jar;

import sleeper.clients.util.command.CommandPipelineRunner;
import sleeper.clients.util.command.CommandUtils;
import sleeper.core.SleeperVersion;
import sleeper.core.deploy.ClientJar;
import sleeper.core.deploy.LambdaJar;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * This class is used to publish built jars to a maven repository.
 * It is called from the publishJarsToRepo.sh script in the scripts/deploy folder.
 * Version defaults to the current SleeperVersion but can be overwritten.
 */
public class PublishJarsToRepo {
    private final Path pathOfJarsDirectory;
    private final String repoUrl;
    private final String m2SettingsServerId;
    private final String version;
    private final CommandPipelineRunner commandRunner;
    private final List<ClientJar> clientJars;
    private final List<LambdaJar> lambdaJars;

    private PublishJarsToRepo(Builder builder) {
        this.pathOfJarsDirectory = requireNonNull(builder.pathOfJarsDirectory, "File path must not be null");
        this.repoUrl = requireNonNull(builder.repoUrl, "Repository URL must not be null");
        this.m2SettingsServerId = requireNonNull(builder.m2SettingsServerId, "M2 settings server Id must not be null");
        this.version = requireNonNull(builder.version, "Version to publish must not be null");
        this.commandRunner = requireNonNull(builder.commandRunner, "Command Runner must not be null");
        this.clientJars = requireNonNull(builder.clientJars, "Client jars must not be null");
        this.lambdaJars = requireNonNull(builder.lambdaJars, "Lambda jars must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * This method sets the path to the jar folder and the repository url then calls the upload method.
     *
     * @param args Should contain the path to the jars folder first and the repisotry url to publish the jars to second.
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 3 || args.length > 3) {
            throw new IllegalArgumentException("Usage: <jars-dir> <repository url> <m2 settings server id>");
        }

        builder()
                .pathOfJarsDirectory(Path.of(args[0]))
                .repoUrl(args[1])
                .m2SettingsServerId(args[2])
                .build()
                .upload();
    }

    /**
     * This method loops through the client jars and lambda jars, publishing each one to the supplied maven repository.
     *
     * @throws InterruptedException if the current thread was interrupted
     * @throws IOException          if soemthing goes wrong with running the maven commands
     */
    public void upload() throws IOException, InterruptedException {
        for (ClientJar clientJar : clientJars) {
            deployJars(clientJar.getFormattedFilename(version), clientJar.getArtifactId());
        }

        for (LambdaJar lambdaJar : lambdaJars) {
            deployJars(lambdaJar.getFormattedFilename(version), lambdaJar.getArtifactId());
        }
    }

    //Requires matching server with auth details in local m2 settings.xml <servers>
    private void deployJars(String filename, String artifactId) throws IOException, InterruptedException {
        commandRunner.run("mvn", "deploy:deploy-file", "-q",
                "-Durl=" + repoUrl,
                "-DrepositoryId=" + m2SettingsServerId, //Requires matching server with auth details in local m2 settings.xml <servers>
                "-Dfile=" + pathOfJarsDirectory.resolve(filename),
                "-DgroupId=sleeper",
                "-DartifactId=" + artifactId,
                "-Dversion=" + version,
                "-DgeneratePom=false");
    }

    /**
     * Builder for publish jars to repo object.
     */
    public static final class Builder {
        private Path pathOfJarsDirectory;
        private String repoUrl;
        private String m2SettingsServerId;
        private String version = SleeperVersion.getVersion();
        private CommandPipelineRunner commandRunner = CommandUtils::runCommandInheritIO;
        private List<ClientJar> clientJars = ClientJar.getAll();
        private List<LambdaJar> lambdaJars = LambdaJar.getAll();

        private Builder() {
        }

        /**
         * Sets the path to the jars directory.
         *
         * @param  pathOfJarsDirectory the path to the jars folder
         * @return                     the builder for method chaining
         */
        public Builder pathOfJarsDirectory(Path pathOfJarsDirectory) {
            this.pathOfJarsDirectory = pathOfJarsDirectory;
            return this;
        }

        /**
         * Sets the maven reposiotry url to publish to.
         *
         * @param  repoUrl the url of the maven repository to publish to
         * @return         the builder for method chaining
         */
        public Builder repoUrl(String repoUrl) {
            this.repoUrl = repoUrl;
            return this;
        }

        /**
         * Sets the id of a server that will be found in a local m2 settings file.
         * Used for authentication.
         *
         * @param  m2SettingsServerId the id of the server in a local m2 settings
         * @return                    the builder for method chaining
         */
        public Builder m2SettingsServerId(String m2SettingsServerId) {
            this.m2SettingsServerId = m2SettingsServerId;
            return this;
        }

        /**
         * Sets the sleeper version to be used to find the correct jars in the jars folder.
         * Defaults to the current sleeper version if not overwritten.
         *
         * @param  version the sleeper version of the jars to publish
         * @return         the builder for method chaining
         */
        public Builder version(String version) {
            this.version = version;
            return this;
        }

        /**
         * Sets the command runner for running the maven commands.
         * Defaults to a generic one unless overwritten.
         *
         * @param  commandRunner the command runner for running maven commands
         * @return               the builder for method chaining
         */
        public Builder commandRunner(CommandPipelineRunner commandRunner) {
            this.commandRunner = commandRunner;
            return this;
        }

        /**
         * Sets the list of client jars to be published.
         *
         * @param  clientJars the list of client jars to be used
         * @return            the builder for method chaining
         */
        public Builder clientJars(List<ClientJar> clientJars) {
            this.clientJars = clientJars;
            return this;
        }

        /**
         * Sets the list of client jars to be published.
         *
         * @param  lambdaJars the list of client jars to be used
         * @return            the builder for method chaining
         */
        public Builder lambdaJars(List<LambdaJar> lambdaJars) {
            this.lambdaJars = lambdaJars;
            return this;
        }

        public PublishJarsToRepo build() {
            return new PublishJarsToRepo(this);
        }
    }

}
