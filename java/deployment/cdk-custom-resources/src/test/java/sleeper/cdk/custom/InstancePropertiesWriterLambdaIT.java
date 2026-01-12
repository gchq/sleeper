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

import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.localstack.test.LocalStackTestBase;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.ACCOUNT;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REGION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;

public class InstancePropertiesWriterLambdaIT extends LocalStackTestBase {

    private InstanceProperties createDefaultProperties(String account, String bucket) {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, "id");
        instanceProperties.set(JARS_BUCKET, "myJars");
        instanceProperties.set(CONFIG_BUCKET, bucket);
        instanceProperties.set(REGION, "region");
        instanceProperties.set(VERSION, "1.2.3");
        instanceProperties.set(SUBNETS, "subnet-12345");
        instanceProperties.set(VPC_ID, "vpc-12345");
        instanceProperties.set(ACCOUNT, account);
        return instanceProperties;
    }

    @Test
    public void shouldUpdateS3BucketOnCreate() throws IOException {
        // Given
        String bucketName = UUID.randomUUID().toString();
        createBucket(bucketName);
        InstancePropertiesWriterLambda lambda = new InstancePropertiesWriterLambda(s3Client, bucketName);

        // When
        InstanceProperties instanceProperties = createDefaultProperties("foo", bucketName);

        HashMap<String, Object> resourceProperties = new HashMap<>();
        resourceProperties.put("properties", instanceProperties.saveAsString());

        CloudFormationCustomResourceEvent event = CloudFormationCustomResourceEvent.builder()
                .withRequestType("Create")
                .withResourceProperties(resourceProperties)
                .build();

        lambda.handleEvent(event, null);

        // Then
        InstanceProperties loadedProperties = S3InstanceProperties.loadFromBucket(s3Client, bucketName);
        assertThat(loadedProperties.get(ACCOUNT)).isEqualTo("foo");

    }

    @Test
    public void shouldUpdateS3BucketOnUpdate() throws IOException {
        // Given
        String bucketName = UUID.randomUUID().toString();
        createBucket(bucketName);
        InstancePropertiesWriterLambda lambda = new InstancePropertiesWriterLambda(s3Client, bucketName);

        putObject(bucketName, S3InstanceProperties.S3_INSTANCE_PROPERTIES_FILE, "foo");

        // When
        InstanceProperties instanceProperties = createDefaultProperties("bar", bucketName);

        HashMap<String, Object> resourceProperties = new HashMap<>();
        resourceProperties.put("properties", instanceProperties.saveAsString());

        CloudFormationCustomResourceEvent event = CloudFormationCustomResourceEvent.builder()
                .withRequestType("Update")
                .withResourceProperties(resourceProperties)
                .build();

        lambda.handleEvent(event, null);

        // Then
        InstanceProperties loadedProperties = S3InstanceProperties.loadFromBucket(s3Client, bucketName);
        assertThat(loadedProperties.get(ACCOUNT)).isEqualTo("bar");
    }

    @Test
    public void shouldUpdateS3BucketAccordingToProperties() throws IOException {
        // Given
        String bucketName = UUID.randomUUID().toString();
        createBucket(bucketName);
        InstancePropertiesWriterLambda lambda = new InstancePropertiesWriterLambda(s3Client, bucketName);
        String alternativeBucket = bucketName + "-alternative";

        createBucket(alternativeBucket);

        // When
        InstanceProperties instanceProperties = createDefaultProperties("foo", alternativeBucket);

        HashMap<String, Object> resourceProperties = new HashMap<>();
        resourceProperties.put("properties", instanceProperties.saveAsString());

        CloudFormationCustomResourceEvent event = CloudFormationCustomResourceEvent.builder()
                .withRequestType("Create")
                .withResourceProperties(resourceProperties)
                .build();

        lambda.handleEvent(event, null);

        // Then
        InstanceProperties loadedProperties = S3InstanceProperties.loadFromBucket(s3Client, alternativeBucket);
        assertThat(loadedProperties.get(ACCOUNT)).isEqualTo("foo");
    }

    @Test
    public void shouldDeleteConfigObjectWhenCalledWithDeleteRequest() throws IOException {
        // Given
        String bucketName = UUID.randomUUID().toString();
        createBucket(bucketName);
        putObject(bucketName, S3InstanceProperties.S3_INSTANCE_PROPERTIES_FILE, "foo");

        // When
        InstanceProperties instanceProperties = createDefaultProperties("foo", bucketName);

        HashMap<String, Object> resourceProperties = new HashMap<>();
        resourceProperties.put("properties", instanceProperties.saveAsString());

        CloudFormationCustomResourceEvent event = CloudFormationCustomResourceEvent.builder()
                .withRequestType("Delete")
                .withResourceProperties(resourceProperties)
                .build();

        InstancePropertiesWriterLambda lambda = new InstancePropertiesWriterLambda(s3Client, bucketName);
        lambda.handleEvent(event, null);

        // Then
        assertThat(listObjectKeys(bucketName)).isEmpty();
    }
}
