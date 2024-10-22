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
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.REGION;

public class StacksForDockerUpload {
    private final String ecrPrefix;
    private final String account;
    private final String region;
    private final String version;
    private final List<StackDockerImage> images;

    private StacksForDockerUpload(Builder builder) {
        ecrPrefix = requireNonNull(builder.ecrPrefix, "ecrPrefix must not be null");
        account = requireNonNull(builder.account, "account must not be null");
        region = requireNonNull(builder.region, "region must not be null");
        version = requireNonNull(builder.version, "version must not be null");
        images = requireNonNull(builder.images, "images must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static StacksForDockerUpload forNewDeployment(InstanceProperties properties, DockerImageConfiguration configuration) {
        return builder().properties(properties).images(configuration.getImagesToUpload(properties)).build();
    }

    public static StacksForDockerUpload forNewDeployment(InstanceProperties properties, String version) {
        return builder().properties(properties).version(version).images(DockerImageConfiguration.getDefault().getImagesToUpload(properties)).build();
    }

    public static StacksForDockerUpload forExistingInstance(InstanceProperties properties) {
        return builder().properties(properties).images(DockerImageConfiguration.getDefault().getImagesToUpload(properties)).build();
    }

    public static Optional<StacksForDockerUpload> forUpdateIfNeeded(InstanceProperties properties, PropertiesDiff diff, DockerImageConfiguration configuration) {
        List<StackDockerImage> images = configuration.getImagesToUploadOnUpdate(properties, diff);
        if (images.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(builder().properties(properties).images(images).build());
        }
    }

    public Builder toBuilder() {
        return builder().ecrPrefix(ecrPrefix).account(account).region(region).version(version).images(images);
    }

    public StacksForDockerUpload withExtraImages(List<StackDockerImage> extraImages) {
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

    public String getVersion() {
        return version;
    }

    public List<StackDockerImage> getImages() {
        return images;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StacksForDockerUpload that = (StacksForDockerUpload) o;
        return Objects.equals(ecrPrefix, that.ecrPrefix)
                && Objects.equals(account, that.account)
                && Objects.equals(region, that.region)
                && Objects.equals(version, that.version)
                && Objects.equals(images, that.images);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ecrPrefix, account, region, version, images);
    }

    @Override
    public String toString() {
        return "StacksForDockerUpload{" +
                "ecrPrefix='" + ecrPrefix + '\'' +
                ", account='" + account + '\'' +
                ", region='" + region + '\'' +
                ", version='" + version + '\'' +
                ", images=" + images +
                '}';
    }

    public static final class Builder {
        private String ecrPrefix;
        private String account;
        private String region;
        private String version;
        private List<StackDockerImage> images;

        private Builder() {
        }

        public Builder properties(InstanceProperties properties) {
            return ecrPrefix(Optional.ofNullable(properties.get(ECR_REPOSITORY_PREFIX)).orElse(properties.get(ID)))
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
            this.version = version;
            return this;
        }

        public Builder images(List<StackDockerImage> images) {
            this.images = images;
            return this;
        }

        public StacksForDockerUpload build() {
            return new StacksForDockerUpload(this);
        }
    }
}
