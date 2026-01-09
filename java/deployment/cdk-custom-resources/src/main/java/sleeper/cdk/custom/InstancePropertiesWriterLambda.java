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
package sleeper.cdk.custom;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configuration.properties.S3InstanceProperties;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * Lambda Function which writes instance properties to an S3 Bucket.
 */
public class InstancePropertiesWriterLambda {
    private static final Logger LOGGER = LoggerFactory.getLogger(InstancePropertiesWriterLambda.class);
    private final S3Client s3Client;
    private final String bucketName;

    public InstancePropertiesWriterLambda() {
        this(S3Client.create(), System.getenv(CONFIG_BUCKET.toEnvironmentVariable()));
    }

    public InstancePropertiesWriterLambda(S3Client s3Client, String bucketName) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
    }

    public void handleEvent(CloudFormationCustomResourceEvent event, Context context) throws IOException {
        String propertiesStr = (String) event.getResourceProperties().get("properties");
        switch (event.getRequestType()) {
            case "Create":
            case "Update":
                updateProperties(propertiesStr);
                break;
            case "Delete":
                deleteProperties(propertiesStr);
                break;
            default:
                throw new IllegalArgumentException("Invalid request type: " + event.getRequestType());
        }
    }

    private void deleteProperties(String propertiesStr) throws IOException {
        String bucketName = readBucketName(propertiesStr);
        LOGGER.info("Deleting from bucket {}", bucketName);
        s3Client.deleteObject(builder -> builder.bucket(bucketName).key(S3InstanceProperties.S3_INSTANCE_PROPERTIES_FILE));
    }

    private void updateProperties(String propertiesStr) throws IOException {
        String bucketName = readBucketName(propertiesStr);
        LOGGER.info("Writing to bucket {}", bucketName);
        s3Client.putObject(builder -> builder.bucket(bucketName).key(S3InstanceProperties.S3_INSTANCE_PROPERTIES_FILE),
                RequestBody.fromString(propertiesStr));
    }

    private String readBucketName(String propertiesStr) throws IOException {
        Properties properties = new Properties();
        properties.load(new StringReader(propertiesStr));
        return properties.getProperty(CONFIG_BUCKET.getPropertyName(), bucketName);
    }
}
