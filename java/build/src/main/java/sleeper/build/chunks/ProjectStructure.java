/*
 * Copyright 2022 Crown Copyright
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
package sleeper.build.chunks;

import sleeper.build.maven.MavenModuleStructure;
import sleeper.build.util.PathUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

public class ProjectStructure {

    private final Path gitHubPropertiesAbsolute;
    private final Path chunksYamlAbsolute;
    private final Path mavenProjectAbsolute;
    private final Path repositoryPath;

    private ProjectStructure(Builder builder) {
        gitHubPropertiesAbsolute = nonNullAbsolute(builder.gitHubPropertiesPath, "gitHubPropertiesPath");
        chunksYamlAbsolute = nonNullAbsolute(builder.chunksYamlPath, "chunksYamlPath");
        mavenProjectAbsolute = nonNullAbsolute(builder.mavenProjectPath, "mavenProjectPath");
        repositoryPath = PathUtils.commonPath(chunksYamlAbsolute, mavenProjectAbsolute);
    }

    private static Path nonNullAbsolute(Path path, String name) {
        return Objects.requireNonNull(path, name + " must not be null")
                .toAbsolutePath();
    }

    public Path getChunksYamlRelative() {
        return repositoryPath.relativize(chunksYamlAbsolute);
    }

    public Path relativizeMavenProjectListPathInRepository(String projectListPath) {
        return repositoryPath.relativize(mavenProjectAbsolute).resolve(projectListPath);
    }

    public ProjectConfiguration loadProjectConfiguration() throws IOException {
        return ProjectConfiguration.fromGitHubAndChunks(gitHubPropertiesAbsolute, chunksYamlAbsolute);
    }

    public MavenModuleStructure loadMavenStructure() throws IOException {
        return MavenModuleStructure.fromProjectBase(mavenProjectAbsolute);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Path gitHubPropertiesPath;
        private Path chunksYamlPath;
        private Path mavenProjectPath;

        private Builder() {
        }

        public Builder gitHubPropertiesPath(Path gitHubPropertiesPath) {
            this.gitHubPropertiesPath = gitHubPropertiesPath;
            return this;
        }

        public Builder chunksYamlPath(Path chunksYamlPath) {
            this.chunksYamlPath = chunksYamlPath;
            return this;
        }

        public Builder mavenProjectPath(Path mavenProjectPath) {
            this.mavenProjectPath = mavenProjectPath;
            return this;
        }

        public ProjectStructure build() {
            return new ProjectStructure(this);
        }
    }
}
