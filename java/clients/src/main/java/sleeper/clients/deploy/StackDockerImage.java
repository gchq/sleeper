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

package sleeper.clients.deploy;

import java.util.Optional;

public class StackDockerImage {
    private final String imageName;
    private final String directoryName;
    private final Optional<String> dockerTarget;
    private final boolean isBuildx;
    private final boolean createEmrServerlessPolicy;

    private StackDockerImage(Builder builder) {
        imageName = builder.imageName;
        directoryName = builder.directoryName;
        isBuildx = builder.isBuildx;
        dockerTarget = Optional.ofNullable(builder.dockerTarget);
        createEmrServerlessPolicy = builder.createEmrServerlessPolicy;
    }

    public static StackDockerImage dockerBuildImage(String imageName) {
        return builder().imageName(imageName)
                .directoryName(imageName).build();
    }

    public static StackDockerImage dockerBuildImage(String imageName, String dockerTarget) {
        return builder().imageName(imageName)
                .directoryName(imageName)
                .target(dockerTarget).build();
    }

    public static StackDockerImage dockerBuildxImage(String imageName) {
        return builder().imageName(imageName)
                .directoryName(imageName).isBuildx(true).build();
    }

    public static StackDockerImage dockerBuildxImage(String imageName, String dockerTarget) {
        return builder().imageName(imageName)
                .directoryName(imageName).isBuildx(true)
                .target(dockerTarget).build();
    }

    public static StackDockerImage emrServerlessImage(String imageName) {
        return builder().imageName(imageName)
                .directoryName(imageName).createEmrServerlessPolicy(true).build();
    }

    public static StackDockerImage emrServerlessImage(String imageName, String dockerTarget) {
        return builder().imageName(imageName)
                .directoryName(imageName).createEmrServerlessPolicy(true)
                .target(dockerTarget).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getImageName() {
        return imageName;
    }

    public Optional<String> getDockerTarget() {
        return dockerTarget;
    }

    public String getDirectoryName() {
        return directoryName;
    }

    public boolean isBuildx() {
        return isBuildx;
    }

    public boolean isCreateEmrServerlessPolicy() {
        return createEmrServerlessPolicy;
    }

    public static final class Builder {
        private String imageName;
        private String directoryName;
        private String dockerTarget;
        private boolean isBuildx;
        private boolean createEmrServerlessPolicy;

        private Builder() {
        }

        public Builder imageName(String imageName) {
            this.imageName = imageName;
            return this;
        }

        public Builder directoryName(String directoryName) {
            this.directoryName = directoryName;
            return this;
        }

        public Builder isBuildx(boolean isBuildx) {
            this.isBuildx = isBuildx;
            return this;
        }

        public Builder createEmrServerlessPolicy(boolean createEmrServerlessPolicy) {
            this.createEmrServerlessPolicy = createEmrServerlessPolicy;
            return this;
        }

        public Builder target(String dockerTarget) {
            this.dockerTarget = dockerTarget;
            return this;
        }

        public StackDockerImage build() {
            return new StackDockerImage(this);
        }
    }
}
