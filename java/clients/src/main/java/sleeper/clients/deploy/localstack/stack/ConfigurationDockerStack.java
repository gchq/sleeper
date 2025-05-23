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
package sleeper.clients.deploy.localstack.stack;

import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.deploy.localstack.TearDownBucket;
import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

public class ConfigurationDockerStack {
    private final InstanceProperties instanceProperties;
    private final S3Client s3Client;

    private ConfigurationDockerStack(InstanceProperties instanceProperties, S3Client s3Client) {
        this.instanceProperties = instanceProperties;
        this.s3Client = s3Client;
    }

    public static ConfigurationDockerStack from(InstanceProperties instanceProperties, S3Client s3Client) {
        return new ConfigurationDockerStack(instanceProperties, s3Client);
    }

    public void deploy() {
        s3Client.createBucket(request -> request.bucket(instanceProperties.get(CONFIG_BUCKET)));
    }

    public void tearDown() {
        TearDownBucket.emptyAndDelete(s3Client, instanceProperties.get(CONFIG_BUCKET));
    }

}
