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

import java.nio.file.Path;
import java.util.Objects;

public class ProjectStructure {

    private final Path mavenPathInRepository;
    private final MavenModuleStructure mavenProject;

    private ProjectStructure(Builder builder) {
        mavenPathInRepository = Objects.requireNonNull(builder.mavenPathInRepository, "mavenPathInRepository must not be null");
        mavenProject = Objects.requireNonNull(builder.mavenProject, "mavenProject must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProjectStructure that = (ProjectStructure) o;
        return mavenPathInRepository.equals(that.mavenPathInRepository) && mavenProject.equals(that.mavenProject);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mavenPathInRepository, mavenProject);
    }

    public static final class Builder {
        private Path mavenPathInRepository;
        private MavenModuleStructure mavenProject;

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public Builder mavenProject(MavenModuleStructure mavenProject) {
            this.mavenProject = mavenProject;
            return this;
        }

        public Builder mavenPathInRepository(Path mavenPathInRepository) {
            this.mavenPathInRepository = mavenPathInRepository;
            return this;
        }

        public ProjectStructure build() {
            return new ProjectStructure(this);
        }
    }
}
