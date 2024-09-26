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

package sleeper.clients.docker.stack;

import com.amazonaws.services.s3.AmazonS3;

import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.clients.docker.Utils.tearDownBucket;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

public class ConfigurationDockerStack implements DockerStack {
    private final AmazonS3 s3Client;
    private final InstanceProperties instanceProperties;

    private ConfigurationDockerStack(Builder builder) {
        s3Client = builder.s3Client;
        instanceProperties = builder.instanceProperties;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static ConfigurationDockerStack from(InstanceProperties instanceProperties, AmazonS3 s3Client) {
        return builder().instanceProperties(instanceProperties).s3Client(s3Client).build();
    }

    public void deploy() {
        s3Client.createBucket(instanceProperties.get(CONFIG_BUCKET));
    }

    public void tearDown() {
        tearDownBucket(s3Client, instanceProperties.get(CONFIG_BUCKET));
    }

    public InstanceProperties getInstanceProperties() {
        return instanceProperties;
    }

    public static final class Builder {
        private AmazonS3 s3Client;
        private InstanceProperties instanceProperties;

        public Builder() {
        }

        public Builder s3Client(AmazonS3 s3Client) {
            this.s3Client = s3Client;
            return this;
        }

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        public ConfigurationDockerStack build() {
            return new ConfigurationDockerStack(this);
        }
    }
}
