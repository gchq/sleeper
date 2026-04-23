/*
 * Copyright 2022-2026 Crown Copyright
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

import sleeper.clients.deploy.container.StackDockerImage;
import sleeper.clients.util.cdk.CdkCommand;
import sleeper.core.deploy.SleeperInstanceConfiguration;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.SleeperCdkDeployment;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class DeployInstanceRequest {

    private final SleeperInstanceConfiguration instanceConfig;
    private final CdkCommand cdkCommand;
    private final SleeperCdkDeployment cdkDeployment;
    private final List<StackDockerImage> extraDockerImages;

    private DeployInstanceRequest(Builder builder) {
        instanceConfig = Objects.requireNonNull(builder.instanceConfig, "instanceConfig must not be null");
        cdkCommand = Objects.requireNonNull(builder.cdkCommand, "cdkCommand must not be null");
        cdkDeployment = Optional.ofNullable(builder.cdkDeployment).orElseGet(() -> inferCdkDeployment(instanceConfig));
        extraDockerImages = Objects.requireNonNull(builder.extraDockerImages, "extraDockerImages must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    private static SleeperCdkDeployment inferCdkDeployment(SleeperInstanceConfiguration instanceConfig) {
        InstanceProperties instanceProperties = instanceConfig.getInstanceProperties();
        if (instanceProperties.isAnyPropertySetStartingWith("sleeper.systemtest")) {
            return SleeperCdkDeployment.DEMONSTRATION;
        } else {
            return SleeperCdkDeployment.STANDARD;
        }
    }

    public SleeperInstanceConfiguration getInstanceConfig() {
        return instanceConfig;
    }

    public SleeperCdkDeployment getCdkDeployment() {
        return cdkDeployment;
    }

    public CdkCommand getCdkCommand() {
        return cdkCommand;
    }

    public List<StackDockerImage> getExtraDockerImages() {
        return extraDockerImages;
    }

    public List<String> getExtraDockerImageNames() {
        return extraDockerImages.stream().map(StackDockerImage::getImageName).toList();
    }

    public static class Builder {
        private SleeperInstanceConfiguration instanceConfig;
        private CdkCommand cdkCommand;
        private SleeperCdkDeployment cdkDeployment = SleeperCdkDeployment.STANDARD;
        private List<StackDockerImage> extraDockerImages = List.of();

        public Builder instanceConfig(SleeperInstanceConfiguration instanceConfig) {
            this.instanceConfig = instanceConfig;
            return this;
        }

        public Builder cdkCommand(CdkCommand cdkCommand) {
            this.cdkCommand = cdkCommand;
            return this;
        }

        public Builder cdkDeployment(SleeperCdkDeployment cdkDeployment) {
            this.cdkDeployment = cdkDeployment;
            return this;
        }

        public Builder inferInstanceType() {
            return cdkDeployment(null);
        }

        public Builder extraDockerImages(List<StackDockerImage> extraDockerImages) {
            this.extraDockerImages = extraDockerImages;
            return this;
        }

        public DeployInstanceRequest build() {
            return new DeployInstanceRequest(this);
        }
    }

}
