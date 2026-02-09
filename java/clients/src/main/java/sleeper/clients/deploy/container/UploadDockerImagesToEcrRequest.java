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

import sleeper.core.properties.instance.InstanceProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;
import static sleeper.core.properties.instance.CommonProperty.ECR_REPOSITORY_PREFIX;

public class UploadDockerImagesToEcrRequest {
    private final String ecrPrefix;
    private final List<StackDockerImage> images;
    private final boolean overwriteExistingTag;

    private UploadDockerImagesToEcrRequest(Builder builder) {
        ecrPrefix = requireNonNull(builder.ecrPrefix, "ecrPrefix must not be null");
        images = requireNonNull(builder.images(), "images must not be null");
        overwriteExistingTag = builder.overwriteExistingTag;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static UploadDockerImagesToEcrRequest forDeployment(InstanceProperties properties, DockerImageConfiguration configuration) {
        return builder().properties(properties).images(configuration.getImagesToUpload(properties)).build();
    }

    public static UploadDockerImagesToEcrRequest forDeployment(InstanceProperties properties) {
        return forDeployment(properties, DockerImageConfiguration.getDefault());
    }

    public Builder toBuilder() {
        return builder().ecrPrefix(ecrPrefix).images(images).overwriteExistingTag(overwriteExistingTag);
    }

    public UploadDockerImagesToEcrRequest withExtraImages(List<StackDockerImage> extraImages) {
        if (extraImages.isEmpty()) {
            return this;
        }
        return toBuilder().extraImages(extraImages).build();
    }

    public String getEcrPrefix() {
        return ecrPrefix;
    }

    public List<StackDockerImage> getImages() {
        return images;
    }

    public boolean isOverwriteExistingTag() {
        return overwriteExistingTag;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof UploadDockerImagesToEcrRequest)) {
            return false;
        }
        UploadDockerImagesToEcrRequest other = (UploadDockerImagesToEcrRequest) obj;
        return Objects.equals(ecrPrefix, other.ecrPrefix) && Objects.equals(images, other.images);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ecrPrefix, images);
    }

    @Override
    public String toString() {
        return "UploadDockerImagesToEcrRequest{ecrPrefix=" + ecrPrefix + ", images=" + images + "}";
    }

    public static final class Builder {
        private String ecrPrefix;
        private List<StackDockerImage> images;
        private List<StackDockerImage> extraImages;
        private boolean overwriteExistingTag;

        private Builder() {
        }

        public Builder properties(InstanceProperties properties) {
            return ecrPrefix(properties.get(ECR_REPOSITORY_PREFIX));
        }

        public Builder ecrPrefix(String ecrPrefix) {
            this.ecrPrefix = ecrPrefix;
            return this;
        }

        public Builder images(List<StackDockerImage> images) {
            this.images = images;
            return this;
        }

        public Builder extraImages(List<StackDockerImage> extraImages) {
            this.extraImages = extraImages;
            return this;
        }

        public Builder overwriteExistingTag(boolean overwriteExistingTag) {
            this.overwriteExistingTag = overwriteExistingTag;
            return this;
        }

        private List<StackDockerImage> images() {
            if (images == null || extraImages == null || extraImages.isEmpty()) {
                return images;
            }
            List<StackDockerImage> newImages = new ArrayList<>(images.size() + extraImages.size());
            newImages.addAll(images);
            newImages.addAll(extraImages);
            return newImages;
        }

        public UploadDockerImagesToEcrRequest build() {
            return new UploadDockerImagesToEcrRequest(this);
        }
    }
}
