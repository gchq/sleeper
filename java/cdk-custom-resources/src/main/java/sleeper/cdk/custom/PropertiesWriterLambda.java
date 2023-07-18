/*
 * Copyright 2022-2023 Crown Copyright
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
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import sleeper.configuration.properties.instance.InstanceProperties;

import java.io.IOException;

import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * Lambda Function which writes properties to an S3 Bucket.
 */
public class PropertiesWriterLambda {
    private final AmazonS3 s3Client;

    public PropertiesWriterLambda() {
        this(AmazonS3ClientBuilder.defaultClient());
    }

    public PropertiesWriterLambda(AmazonS3 s3Client) {
        this.s3Client = s3Client;
    }

    public void handleEvent(CloudFormationCustomResourceEvent event, Context context) throws IOException {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromString((String) event.getResourceProperties().get("properties"));
        switch (event.getRequestType()) {
            case "Create":
            case "Update":
                updateProperties(instanceProperties);
                break;
            case "Delete":
                deleteProperties(instanceProperties);
                break;
            default:
                throw new IllegalArgumentException("Invalid request type: " + event.getRequestType());
        }
    }

    private void deleteProperties(InstanceProperties instanceProperties) {
        String bucketName = instanceProperties.get(CONFIG_BUCKET);
        s3Client.deleteObject(bucketName, InstanceProperties.S3_INSTANCE_PROPERTIES_FILE);
    }

    private void updateProperties(InstanceProperties instanceProperties) {
        try {
            instanceProperties.saveToS3(s3Client);
        } catch (IOException e) {
            throw new RuntimeException("Failed to update sleeper properties", e);
        }
    }
}
