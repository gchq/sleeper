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

import org.apache.commons.lang3.EnumUtils;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.validation.OptionalStack;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.configuration.properties.instance.CommonProperty.ECR_REPOSITORY_PREFIX;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.configuration.properties.instance.CommonProperty.REGION;

public class StacksForDockerUpload {
    private final String ecrPrefix;
    private final String account;
    private final String region;
    private final String version;
    private final List<OptionalStack> stacks;

    private StacksForDockerUpload(Builder builder) {
        ecrPrefix = requireNonNull(builder.ecrPrefix, "ecrPrefix must not be null");
        account = requireNonNull(builder.account, "account must not be null");
        region = requireNonNull(builder.region, "region must not be null");
        version = requireNonNull(builder.version, "version must not be null");
        stacks = requireNonNull(builder.stacks, "stacks must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static StacksForDockerUpload from(InstanceProperties properties) {
        return from(properties, properties.get(VERSION));
    }

    public static StacksForDockerUpload from(InstanceProperties properties, String version) {
        return builder()
                .ecrPrefix(Optional.ofNullable(properties.get(ECR_REPOSITORY_PREFIX))
                        .orElse(properties.get(ID)))
                .account(properties.get(ACCOUNT))
                .region(properties.get(REGION))
                .version(version)
                .stacks(properties.getList(OPTIONAL_STACKS)).build();
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

    public List<OptionalStack> getStacks() {
        return stacks;
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
                && Objects.equals(stacks, that.stacks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ecrPrefix, account, region, version, stacks);
    }

    @Override
    public String toString() {
        return "StacksForDockerUpload{" +
                "ecrPrefix='" + ecrPrefix + '\'' +
                ", account='" + account + '\'' +
                ", region='" + region + '\'' +
                ", version='" + version + '\'' +
                ", stacks=" + stacks +
                '}';
    }

    public static final class Builder {
        private String ecrPrefix;
        private String account;
        private String region;
        private String version;
        private List<OptionalStack> stacks;

        private Builder() {
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

        public Builder stacks(List<String> stacks) {
            this.stacks = stacks.stream()
                    .map(str -> EnumUtils.getEnumIgnoreCase(OptionalStack.class, str))
                    .collect(toUnmodifiableList());
            return this;
        }

        public StacksForDockerUpload build() {
            return new StacksForDockerUpload(this);
        }
    }
}
