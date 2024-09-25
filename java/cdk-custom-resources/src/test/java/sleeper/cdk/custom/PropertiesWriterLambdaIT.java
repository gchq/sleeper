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
package sleeper.cdk.custom;

import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.s3properties.S3InstanceProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.properties.instance.InstanceProperties;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.REGION;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;

@Testcontainers
public class PropertiesWriterLambdaIT {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3);

    private AmazonS3 createClient() {
        return buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    }

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
        AmazonS3 client = createClient();
        String bucketName = UUID.randomUUID().toString();
        client.createBucket(bucketName);
        PropertiesWriterLambda propertiesWriterLambda = new PropertiesWriterLambda(client, bucketName);

        // When
        InstanceProperties instanceProperties = createDefaultProperties("foo", bucketName);

        HashMap<String, Object> resourceProperties = new HashMap<>();
        resourceProperties.put("properties", instanceProperties.saveAsString());

        CloudFormationCustomResourceEvent event = CloudFormationCustomResourceEvent.builder()
                .withRequestType("Create")
                .withResourceProperties(resourceProperties)
                .build();

        propertiesWriterLambda.handleEvent(event, null);

        // Then
        InstanceProperties loadedProperties = S3InstanceProperties.loadFromBucket(client, bucketName);
        assertThat(loadedProperties.get(ACCOUNT)).isEqualTo("foo");

        client.shutdown();

    }

    @Test
    public void shouldUpdateS3BucketOnUpdate() throws IOException {
        // Given
        AmazonS3 client = createClient();
        String bucketName = UUID.randomUUID().toString();
        client.createBucket(bucketName);
        PropertiesWriterLambda propertiesWriterLambda = new PropertiesWriterLambda(client, bucketName);

        client.putObject(bucketName, S3InstanceProperties.S3_INSTANCE_PROPERTIES_FILE, "foo");

        // When
        InstanceProperties instanceProperties = createDefaultProperties("bar", bucketName);

        HashMap<String, Object> resourceProperties = new HashMap<>();
        resourceProperties.put("properties", instanceProperties.saveAsString());

        CloudFormationCustomResourceEvent event = CloudFormationCustomResourceEvent.builder()
                .withRequestType("Update")
                .withResourceProperties(resourceProperties)
                .build();

        propertiesWriterLambda.handleEvent(event, null);

        // Then
        InstanceProperties loadedProperties = S3InstanceProperties.loadFromBucket(client, bucketName);
        assertThat(loadedProperties.get(ACCOUNT)).isEqualTo("bar");

        client.shutdown();
    }

    @Test
    public void shouldUpdateS3BucketAccordingToProperties() throws IOException {
        // Given
        AmazonS3 client = createClient();
        String bucketName = UUID.randomUUID().toString();
        client.createBucket(bucketName);
        PropertiesWriterLambda propertiesWriterLambda = new PropertiesWriterLambda(client, bucketName);
        String alternativeBucket = bucketName + "-alternative";

        client.createBucket(alternativeBucket);

        // When
        InstanceProperties instanceProperties = createDefaultProperties("foo", alternativeBucket);

        HashMap<String, Object> resourceProperties = new HashMap<>();
        resourceProperties.put("properties", instanceProperties.saveAsString());

        CloudFormationCustomResourceEvent event = CloudFormationCustomResourceEvent.builder()
                .withRequestType("Create")
                .withResourceProperties(resourceProperties)
                .build();

        propertiesWriterLambda.handleEvent(event, null);

        // Then
        InstanceProperties loadedProperties = S3InstanceProperties.loadFromBucket(client, alternativeBucket);
        assertThat(loadedProperties.get(ACCOUNT)).isEqualTo("foo");

        client.shutdown();
    }

    @Test
    public void shouldDeleteConfigObjectWhenCalledWithDeleteRequest() throws IOException {
        // Given
        AmazonS3 client = createClient();
        String bucketName = UUID.randomUUID().toString();
        client.createBucket(bucketName);
        client.putObject(bucketName, S3InstanceProperties.S3_INSTANCE_PROPERTIES_FILE, "foo");

        // When
        InstanceProperties instanceProperties = createDefaultProperties("foo", bucketName);

        HashMap<String, Object> resourceProperties = new HashMap<>();
        resourceProperties.put("properties", instanceProperties.saveAsString());

        CloudFormationCustomResourceEvent event = CloudFormationCustomResourceEvent.builder()
                .withRequestType("Delete")
                .withResourceProperties(resourceProperties)
                .build();

        PropertiesWriterLambda lambda = new PropertiesWriterLambda(client, bucketName);
        lambda.handleEvent(event, null);

        // Then
        assertThat(client.listObjects(bucketName).getObjectSummaries()).isEmpty();
        client.shutdown();
    }
}
