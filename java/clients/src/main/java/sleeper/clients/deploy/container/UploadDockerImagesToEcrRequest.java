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

import sleeper.clients.admin.properties.PropertiesDiff;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.core.properties.instance.CommonProperty.ECR_REPOSITORY_PREFIX;
import static sleeper.core.properties.instance.CommonProperty.REGION;

public class UploadDockerImagesToEcrRequest {
    private final String ecrPrefix;
    private final String account;
    private final String region;
    private final List<StackDockerImage> images;
    private final String repositoryHost;

    private UploadDockerImagesToEcrRequest(Builder builder) {
        ecrPrefix = requireNonNull(builder.ecrPrefix, "ecrPrefix must not be null");
        account = requireNonNull(builder.account, "account must not be null");
        region = requireNonNull(builder.region, "region must not be null");
        images = requireNonNull(builder.images, "images must not be null");
        repositoryHost = String.format("%s.dkr.ecr.%s.amazonaws.com", account, region);
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

    public static Optional<UploadDockerImagesToEcrRequest> forUpdateIfNeeded(InstanceProperties properties, PropertiesDiff diff, DockerImageConfiguration configuration) {
        List<StackDockerImage> images = configuration.getImagesToUploadOnUpdate(properties, diff);
        if (images.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(builder().properties(properties).images(images).build());
        }
    }

    public Builder toBuilder() {
        return builder().ecrPrefix(ecrPrefix).account(account).region(region).images(images);
    }

    public UploadDockerImagesToEcrRequest withExtraImages(List<StackDockerImage> extraImages) {
        if (extraImages.isEmpty()) {
            return this;
        }
        List<StackDockerImage> newImages = new ArrayList<>(images.size() + extraImages.size());
        newImages.addAll(images);
        newImages.addAll(extraImages);
        return toBuilder().images(newImages).build();
    }

    public String getEcrPrefix() {
        return ecrPrefix;
    }

    public String getAccount() {
        return account;
    }

    public String getRegion() {
        return region;
    }

    public List<StackDockerImage> getImages() {
        return images;
    }

    public String getRepositoryHost() {
        return repositoryHost;
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
        return Objects.equals(ecrPrefix, other.ecrPrefix) && Objects.equals(account, other.account) && Objects.equals(region, other.region) && Objects.equals(images, other.images);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ecrPrefix, account, region, images);
    }

    @Override
    public String toString() {
        return "UploadDockerImagesToEcrRequest{ecrPrefix=" + ecrPrefix + ", account=" + account + ", region=" + region + ", images=" + images + "}";
    }

    public static final class Builder {
        private String ecrPrefix;
        private String account;
        private String region;
        private List<StackDockerImage> images;

        private Builder() {
        }

        public Builder properties(InstanceProperties properties) {
            return ecrPrefix(properties.get(ECR_REPOSITORY_PREFIX))
                    .account(properties.get(ACCOUNT))
                    .region(properties.get(REGION))
                    .version(properties.get(VERSION));
        }

        public Builder ecrPrefix(String ecrPrefix) {
            this.ecrPrefix = ecrPrefix;
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
            return this;
        }

        public Builder images(List<StackDockerImage> images) {
            this.images = images;
            return this;
        }

        public UploadDockerImagesToEcrRequest build() {
            return new UploadDockerImagesToEcrRequest(this);
        }
    }
}
