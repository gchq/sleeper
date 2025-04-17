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
package sleeper.configurationv2.properties;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.local.SaveLocalProperties;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

import static sleeper.core.properties.PropertiesUtils.loadProperties;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * Saves and loads instance properties in AWS S3.
 */
public class S3InstanceProperties {

    public static final Logger LOGGER = LoggerFactory.getLogger(S3InstanceProperties.class);

    public static final String S3_INSTANCE_PROPERTIES_FILE = "instance.properties";

    private S3InstanceProperties() {
    }

    /**
     * Loads and validates instance properties from the config bucket of the given Sleeper instance.
     *
     * @param  s3Client   the S3 client
     * @param  instanceId the Sleeper instance ID
     * @return            the loaded instance properties
     */
    public static InstanceProperties loadGivenInstanceId(S3Client s3Client, String instanceId) {
        return InstanceProperties.createAndValidate(loadPropertiesGivenInstanceId(s3Client, instanceId));
    }

    /**
     * Loads instance properties from the config bucket of the given Sleeper instance, with no validation.
     *
     * @param  s3Client   the S3 client
     * @param  instanceId the Sleeper instance ID
     * @return            the loaded instance properties
     */
    public static InstanceProperties loadGivenInstanceIdNoValidation(S3Client s3Client, String instanceId) {
        return InstanceProperties.createWithoutValidation(loadPropertiesGivenInstanceId(s3Client, instanceId));
    }

    /**
     * Loads and validates instance properties from the given S3 bucket.
     *
     * @param  s3Client the S3 client
     * @param  bucket   the bucket name
     * @return          the loaded instance properties
     */
    public static InstanceProperties loadFromBucket(S3Client s3Client, String bucket) {
        return InstanceProperties.createAndValidate(loadPropertiesFromBucket(s3Client, bucket));
    }

    /**
     * Saves instance properties to the config bucket. This will only work if the given properties were originally
     * loaded from a deployed instance, as otherwise the config bucket property will not be set.
     *
     * @param s3Client   the S3 client
     * @param properties the instance properties
     */
    public static void saveToS3(S3Client s3Client, InstanceProperties properties) {
        String bucket = properties.get(CONFIG_BUCKET);
        LOGGER.debug("Uploading config to bucket {}", bucket);
        s3Client.putObject(builder -> builder.bucket(bucket).key(S3_INSTANCE_PROPERTIES_FILE),
                RequestBody.fromString(properties.toString()));

        //s3Client.putObject(bucket, S3_INSTANCE_PROPERTIES_FILE, properties.saveAsString());
        LOGGER.info("Saved instance properties to bucket {}, key {}", bucket, S3_INSTANCE_PROPERTIES_FILE);
    }

    /**
     * Reloads and validates instance properties from the config bucket. This will only work if the given properties
     * were originally loaded from a deployed instance, as otherwise the config bucket property will not be set.
     *
     * @param s3Client   the S3 client
     * @param properties the instance properties
     */
    public static void reload(S3Client s3Client, InstanceProperties properties) {
        properties.resetAndValidate(loadPropertiesFromBucket(s3Client, properties.get(CONFIG_BUCKET)));
    }

    /**
     * Reloads and validates instance properties from the config bucket of the given Sleeper instance.
     *
     * @param s3Client   the S3 client
     * @param properties the instance properties
     * @param instanceId the Sleeper instance ID
     */
    public static void reloadGivenInstanceId(S3Client s3Client, InstanceProperties properties, String instanceId) {
        properties.resetAndValidate(loadPropertiesFromBucket(s3Client, InstanceProperties.getConfigBucketFromInstanceId(instanceId)));
    }

    /**
     * Retrieves instance and table properties from the config bucket and saves the configuration to the local file
     * system.
     *
     * @param  s3          the S3 client
     * @param  dynamoDB    the DynamoDB client
     * @param  instanceId  the Sleeper instance ID
     * @param  directory   the directory to save the configuration to
     * @return             the instance properties
     * @throws IOException if the configuration could not be saved to the local file system
     */
    public static InstanceProperties saveToLocalWithTableProperties(
            S3Client s3, AmazonDynamoDB dynamoDB, String instanceId, Path directory) throws IOException {
        InstanceProperties instanceProperties = loadGivenInstanceId(s3, instanceId);
        SaveLocalProperties.saveToDirectory(directory, instanceProperties,
                S3TableProperties.createStore(instanceProperties, s3, dynamoDB)
                        .streamAllTables());
        return instanceProperties;
    }

    private static Properties loadPropertiesGivenInstanceId(S3Client s3Client, String instanceId) {
        return loadPropertiesFromBucket(s3Client, InstanceProperties.getConfigBucketFromInstanceId(instanceId));
    }

    private static Properties loadPropertiesFromBucket(S3Client s3Client, String bucket) {
        return loadProperties(s3Client.getObjectAsBytes(builder -> builder.bucket(bucket)
                .key(S3_INSTANCE_PROPERTIES_FILE)).asUtf8String());
    }

}
