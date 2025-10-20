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
package sleeper.clients.deploy;

import sleeper.clients.deploy.container.StackDockerImage;
import sleeper.clients.util.cdk.CdkCommand;
import sleeper.clients.util.cdk.InvokeCdkForInstance;
import sleeper.core.deploy.DeployInstanceConfiguration;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DeployInstanceRequest {

    private final DeployInstanceConfiguration instanceConfig;
    private final CdkCommand cdkCommand;
    private final InvokeCdkForInstance.Type instanceType;
    private final List<StackDockerImage> extraDockerImages;

    private DeployInstanceRequest(Builder builder) {
        instanceConfig = requireNonNull(builder.instanceConfig, "instanceConfig must not be null");
        cdkCommand = requireNonNull(builder.cdkCommand, "cdkCommand must not be null");
        instanceType = Optional.ofNullable(builder.instanceType).orElseGet(() -> inferInstanceType(instanceConfig));
        extraDockerImages = requireNonNull(builder.extraDockerImages, "extraDockerImages must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    private static InvokeCdkForInstance.Type inferInstanceType(DeployInstanceConfiguration instanceConfig) {
        InstanceProperties instanceProperties = instanceConfig.getInstanceProperties();
        if (instanceProperties.isAnyPropertySetStartingWith("sleeper.systemtest")) {
            return InvokeCdkForInstance.Type.SYSTEM_TEST;
        } else {
            return InvokeCdkForInstance.Type.STANDARD;
        }
    }

    public DeployInstanceConfiguration getInstanceConfig() {
        return instanceConfig;
    }

    public InvokeCdkForInstance.Type getInstanceType() {
        return instanceType;
    }

    public CdkCommand getCdkCommand() {
        return cdkCommand;
    }

    public List<StackDockerImage> getExtraDockerImages() {
        return extraDockerImages;
    }

    public static class Builder {
        private DeployInstanceConfiguration instanceConfig;
        private CdkCommand cdkCommand;
        private InvokeCdkForInstance.Type instanceType = InvokeCdkForInstance.Type.STANDARD;
        private List<StackDockerImage> extraDockerImages = List.of();

        public Builder instanceConfig(DeployInstanceConfiguration instanceConfig) {
            this.instanceConfig = instanceConfig;
            return this;
        }

        public Builder cdkCommand(CdkCommand cdkCommand) {
            this.cdkCommand = cdkCommand;
            return this;
        }

        public Builder instanceType(InvokeCdkForInstance.Type instanceType) {
            this.instanceType = instanceType;
            return this;
        }

        public Builder inferInstanceType() {
            return instanceType(null);
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
