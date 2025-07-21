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

import static java.util.Objects.requireNonNull;

public class PublishJarsToRepo {
    private final Path filePath;
    private final String repoUrl;
    private final String version;
    private final CommandPipelineRunner commandRunner;

    private PublishJarsToRepo(Builder builder) {
        this.filePath = requireNonNull(builder.filePath, "File path must not be null");
        this.repoUrl = requireNonNull(builder.repoUrl, "Repository URL must not be null");
        this.version = requireNonNull(builder.version, "Version to publish must not be null");
        this.commandRunner = requireNonNull(builder.commandRunner, "Command Runner must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static void main(String[] args) throws Exception {
        builder()
                .filePath(Path.of(args[0] + "/"))
                .repoUrl(args[1])
                .build()
                .upload();

    }

    public void upload() {
        for (ClientJar clientJar : ClientJar.getAll()) {
            deployJars(clientJar.getFilenameFormat(), clientJar.getArtifactId());
        }

        for (LambdaJar lambdaJar : LambdaJar.getAll()) {
            deployJars(lambdaJar.getFilenameFormat(), lambdaJar.getArtifactId());
        }
    }

    //Requires matching server with auth details in local m2 settings.xml <servers>
    private void deployJars(String filename, String imageName) {
        try {
            commandRunner.run("mvn", "deploy:deploy-file", "-q",
                    "-Durl=" + repoUrl,
                    "-DrepositoryId=repo.id", //Requires matching server with auth details in local m2 settings.xml <servers>
                    "-Dfile=" + filePath.resolve(String.format(filename, version)),
                    "-DgroupId=sleeper",
                    "-DartifactId=" + imageName,
                    "-Dversion=" + version,
                    "-DgeneratePom=false");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static final class Builder {
        private Path filePath;
        private String repoUrl;
        private String version = SleeperVersion.getVersion();
        CommandPipelineRunner commandRunner = CommandUtils::runCommandInheritIO;

        private Builder() {
        }

        public Builder filePath(Path filePath) {
            this.filePath = filePath;
            return this;
        }

        public Builder repoUrl(String repoUrl) {
            this.repoUrl = repoUrl;
            return this;
        }

        public Builder version(String version) {
            this.version = version;
            return this;
        }

        public Builder commandRunner(CommandPipelineRunner commandRunner) {
            this.commandRunner = commandRunner;
            return this;
        }

        public PublishJarsToRepo build() {
            return new PublishJarsToRepo(this);
        }
    }

}
