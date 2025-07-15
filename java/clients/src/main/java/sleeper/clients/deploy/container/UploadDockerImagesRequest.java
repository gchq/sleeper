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
package sleeper.clients.deploy.container;

import java.util.List;

public class UploadDockerImagesRequest {

    private final String repositoryPrefix;
    private final List<StackDockerImage> imagesToUpload;

    public UploadDockerImagesRequest(Builder builder) {
        repositoryPrefix = builder.repositoryPrefix;
        imagesToUpload = builder.imagesToUpload;
    }

    public static UploadDockerImagesRequest allImagesToRepository(String repositoryPrefix) {
        return allImagesToRepository(repositoryPrefix, DockerImageConfiguration.getDefault());
    }

    public static UploadDockerImagesRequest allImagesToRepository(String repositoryPrefix, DockerImageConfiguration imageConfig) {
        return builder().repositoryPrefix(repositoryPrefix).imagesToUpload(imageConfig.getAllImagesToUpload()).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getRepositoryPrefix() {
        return repositoryPrefix;
    }

    public List<StackDockerImage> getImagesToUpload() {
        return imagesToUpload;
    }

    public static class Builder {
        private String repositoryPrefix;
        private List<StackDockerImage> imagesToUpload;

        public Builder repositoryPrefix(String repositoryPrefix) {
            this.repositoryPrefix = repositoryPrefix;
            return this;
        }

        public Builder imagesToUpload(List<StackDockerImage> imagesToUpload) {
            this.imagesToUpload = imagesToUpload;
            return this;
        }

        public UploadDockerImagesRequest build() {
            return new UploadDockerImagesRequest(this);
        }
    }

}
