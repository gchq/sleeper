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
package sleeper.systemtest.configuration;

import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.PropertyGroup;
import sleeper.core.properties.SleeperPropertiesPrettyPrinter;
import sleeper.core.properties.SleeperPropertiesPrettyPrinter.Builder;
import sleeper.core.properties.SleeperPropertyIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.InstanceProperty;
import sleeper.core.properties.instance.InstancePropertyGroup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static sleeper.core.properties.PropertiesUtils.loadProperties;

/**
 * Holds properties for a Sleeper instance as well as properties needed to run system test data generation tasks.
 */
public class SystemTestProperties extends InstanceProperties {

    static final SleeperPropertyIndex<InstanceProperty> COMBINED_INDEX = createCombinedPropertyIndex();
    static final List<PropertyGroup> COMBINED_GROUPS = createCombinedGroups();

    public SystemTestProperties() {
        super();
    }

    public SystemTestProperties(Properties properties) {
        super(properties);
    }

    public static SystemTestProperties from(InstanceProperties instanceProperties) {
        return new SystemTestProperties(instanceProperties.getProperties());
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

    public static SystemTestProperties loadFromS3GivenAccountAndInstanceId(S3Client s3Client, String accountName, String instanceId) {
        return loadFromBucket(s3Client, InstanceProperties.getConfigBucketFromAccountAndInstanceId(accountName, instanceId));
    }

    private static SleeperPropertyIndex<InstanceProperty> createCombinedPropertyIndex() {
        SleeperPropertyIndex<InstanceProperty> index = new SleeperPropertyIndex<>();
        index.addAll(InstanceProperty.getAll());
        index.addAll(SystemTestProperty.getAll());
        return index;
    }

    private static List<PropertyGroup> createCombinedGroups() {
        List<PropertyGroup> groups = new ArrayList<>();
        groups.add(SystemTestProperty.SYSTEM_TEST_GROUP);
        groups.addAll(InstancePropertyGroup.getAll());
        return Collections.unmodifiableList(groups);
    }

    @Override
    public SleeperPropertyIndex<InstanceProperty> getPropertiesIndex() {
        return COMBINED_INDEX;
    }

    public SystemTestPropertyValues testPropertiesOnly() {
        return this::get;
    }

    @Override
    protected Builder<InstanceProperty> prettyPrinterBuilder() {
        return createSystemTestPrettyPrinterBuilder();
    }

    /**
     * Creates a builder for a printer to be used to display all system test and instance properties.
     *
     * @return the pretty printer
     */
    public static SleeperPropertiesPrettyPrinter.Builder<InstanceProperty> createSystemTestPrettyPrinterBuilder() {
        return SleeperPropertiesPrettyPrinter.builder()
                .properties(COMBINED_INDEX.getAll(), COMBINED_GROUPS);
    }
}
