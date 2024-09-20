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
package sleeper.systemtest.configuration;

import com.amazonaws.services.s3.AmazonS3;

import sleeper.configuration.properties.SleeperPropertyIndex;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.InstanceProperty;
import sleeper.configuration.properties.instance.S3InstanceProperties;

import java.util.Properties;

import static sleeper.configuration.properties.PropertiesUtils.loadProperties;

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

    public static SystemTestProperties loadFromBucket(AmazonS3 s3Client, String bucket) {
        SystemTestProperties properties = new SystemTestProperties();
        properties.resetAndValidate(loadProperties(s3Client.getObjectAsString(bucket, S3InstanceProperties.S3_INSTANCE_PROPERTIES_FILE)));
        return properties;
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
