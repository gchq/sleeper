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
package sleeper.systemtest.configuration;

import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.core.properties.SleeperPropertyIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.InstanceProperty;

import java.util.Properties;

import static sleeper.core.properties.PropertiesUtils.loadProperties;

/**
 * Holds properties for a Sleeper instance as well as properties needed to run system test data generation tasks.
 */
public class SystemTestProperties extends InstanceProperties {

    static final SleeperPropertyIndex<InstanceProperty> PROPERTY_INDEX = createPropertyIndex();

    public SystemTestProperties() {
        super();
    }

    public SystemTestProperties(Properties properties) {
        super(properties);
    }

    public static SystemTestProperties loadFromBucket(S3Client s3Client, String bucket) {
        SystemTestProperties properties = new SystemTestProperties();

        properties.resetAndValidate(
                loadProperties(
                        s3Client.getObject(
                                GetObjectRequest.builder()
                                        .bucket(bucket)
                                        .key(S3InstanceProperties.S3_INSTANCE_PROPERTIES_FILE)
                                        .build(),
                                ResponseTransformer.toBytes()).asUtf8String()));

        return properties;
    }

    public static SystemTestProperties loadFromS3GivenInstanceId(S3Client s3Client, String instanceId) {
        return loadFromBucket(s3Client, InstanceProperties.getConfigBucketFromInstanceId(instanceId));
    }

    private static SleeperPropertyIndex<InstanceProperty> createPropertyIndex() {
        SleeperPropertyIndex<InstanceProperty> index = new SleeperPropertyIndex<>();
        index.addAll(InstanceProperty.getAll());
        index.addAll(SystemTestProperty.getAll());
        return index;
    }

    @Override
    public SleeperPropertyIndex<InstanceProperty> getPropertiesIndex() {
        return PROPERTY_INDEX;
    }

    public SystemTestPropertyValues testPropertiesOnly() {
        return this::get;
    }
}
