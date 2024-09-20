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
package sleeper.configuration.properties.instance;

import com.amazonaws.services.s3.AmazonS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Properties;

import static sleeper.configuration.properties.PropertiesUtils.loadProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

public class S3InstanceProperties {

    public static final Logger LOGGER = LoggerFactory.getLogger(S3InstanceProperties.class);

    public static final String S3_INSTANCE_PROPERTIES_FILE = "instance.properties";

    private S3InstanceProperties() {
    }

    public static InstanceProperties loadGivenInstanceId(AmazonS3 s3Client, String instanceId) {
        return InstanceProperties.createAndValidate(loadPropertiesGivenInstanceId(s3Client, instanceId));
    }

    public static InstanceProperties loadGivenInstanceIdNoValidation(AmazonS3 s3Client, String instanceId) {
        return InstanceProperties.createWithoutValidation(loadPropertiesGivenInstanceId(s3Client, instanceId));
    }

    public static InstanceProperties loadFromBucket(AmazonS3 s3Client, String bucket) {
        return InstanceProperties.createAndValidate(loadPropertiesFromBucket(s3Client, bucket));
    }

    public static void saveToS3(AmazonS3 s3Client, InstanceProperties properties) {
        String bucket = properties.get(CONFIG_BUCKET);
        LOGGER.debug("Uploading config to bucket {}", bucket);
        s3Client.putObject(bucket, S3_INSTANCE_PROPERTIES_FILE, properties.saveAsString());
        LOGGER.info("Saved instance properties to bucket {}, key {}", bucket, S3_INSTANCE_PROPERTIES_FILE);
    }

    private static Properties loadPropertiesGivenInstanceId(AmazonS3 s3Client, String instanceId) {
        return loadPropertiesFromBucket(s3Client, getConfigBucketFromInstanceId(instanceId));
    }

    private static Properties loadPropertiesFromBucket(AmazonS3 s3Client, String bucket) {
        return loadProperties(s3Client.getObjectAsString(bucket, S3_INSTANCE_PROPERTIES_FILE));
    }

    public static String getConfigBucketFromInstanceId(String instanceId) {
        return String.join("-", "sleeper", instanceId, "config").toLowerCase(Locale.ROOT);
    }

}
