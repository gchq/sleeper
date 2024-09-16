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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.SleeperPropertyIndex;
import sleeper.configuration.properties.format.SleeperPropertiesPrettyPrinter;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.InstancePropertyGroup;

import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static sleeper.configuration.properties.PropertiesUtils.loadProperties;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_BUCKET_NAME;

public class SystemTestStandaloneProperties
        extends SleeperProperties<SystemTestProperty>
        implements SystemTestPropertyValues, SystemTestPropertySetter {
    private static final Logger LOGGER = LoggerFactory.getLogger(SystemTestStandaloneProperties.class);

    public SystemTestStandaloneProperties() {
    }

    public SystemTestStandaloneProperties(Properties properties) {
        super(properties);
    }

    public static SystemTestStandaloneProperties fromS3(AmazonS3 s3Client, String bucket) {
        SystemTestStandaloneProperties properties = new SystemTestStandaloneProperties();
        properties.loadFromS3(s3Client, bucket, InstanceProperties.S3_INSTANCE_PROPERTIES_FILE);
        return properties;
    }

    public static SystemTestStandaloneProperties fromS3GivenDeploymentId(AmazonS3 s3Client, String deploymentId) {
        return fromS3(s3Client, buildSystemTestBucketName(deploymentId));
    }

    public static SystemTestStandaloneProperties fromFile(Path propertiesFile) {
        return new SystemTestStandaloneProperties(loadProperties(propertiesFile));
    }

    public void saveToS3(AmazonS3 s3Client) {
        saveToS3(s3Client, get(SYSTEM_TEST_BUCKET_NAME), InstanceProperties.S3_INSTANCE_PROPERTIES_FILE);
        LOGGER.info("Saved system test properties to bucket {}, key {}",
                get(SYSTEM_TEST_BUCKET_NAME), InstanceProperties.S3_INSTANCE_PROPERTIES_FILE);
    }

    @Override
    public String get(SystemTestProperty property) {
        return compute(property, value -> property.computeValue(value, other -> get((SystemTestProperty) other)));
    }

    @Override
    public SleeperPropertyIndex<SystemTestProperty> getPropertiesIndex() {
        return SystemTestProperty.Index.INSTANCE;
    }

    @Override
    protected SleeperPropertiesPrettyPrinter<SystemTestProperty> getPrettyPrinter(PrintWriter writer) {
        return SleeperPropertiesPrettyPrinter.builder()
                .properties(SystemTestProperty.getAll(), List.of(InstancePropertyGroup.COMMON))
                .build();
    }

    public static String buildSystemTestBucketName(String deploymentId) {
        return String.join("-", "sleeper", deploymentId, "system", "test")
                .toLowerCase(Locale.ROOT);
    }
}
