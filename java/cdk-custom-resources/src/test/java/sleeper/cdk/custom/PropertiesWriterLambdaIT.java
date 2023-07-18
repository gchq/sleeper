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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.CommonTestConstants;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ACCOUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNETS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VPC_ID;

@Testcontainers
public class PropertiesWriterLambdaIT {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3);

    private AmazonS3 createClient() {
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        localStackContainer.getEndpointOverride(LocalStackContainer.Service.S3).toString(),
                        localStackContainer.getRegion()))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                        localStackContainer.getAccessKey(), localStackContainer.getSecretKey())))
                .build();
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
        PropertiesWriterLambda propertiesWriterLambda = new PropertiesWriterLambda(client);

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
        InstanceProperties loadedProperties = new InstanceProperties();
        loadedProperties.loadFromS3(client, bucketName);
        assertThat(loadedProperties.get(ACCOUNT)).isEqualTo("foo");

        client.shutdown();

    }

    @Test
    public void shouldUpdateS3BucketOnUpdate() throws IOException {
        // Given
        AmazonS3 client = createClient();
        String bucketName = UUID.randomUUID().toString();
        client.createBucket(bucketName);
        PropertiesWriterLambda propertiesWriterLambda = new PropertiesWriterLambda(client);

        client.putObject(bucketName, "config", "foo");

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
        InstanceProperties loadedProperties = new InstanceProperties();
        loadedProperties.loadFromS3(client, bucketName);
        assertThat(loadedProperties.get(ACCOUNT)).isEqualTo("bar");

        client.shutdown();
    }

    @Test
    public void shouldUpdateS3BucketAccordingToProperties() throws IOException {
        // Given
        AmazonS3 client = createClient();
        String bucketName = UUID.randomUUID().toString();
        client.createBucket(bucketName);
        PropertiesWriterLambda propertiesWriterLambda = new PropertiesWriterLambda(client);
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
        InstanceProperties loadedProperties = new InstanceProperties();
        loadedProperties.loadFromS3(client, alternativeBucket);
        assertThat(loadedProperties.get(ACCOUNT)).isEqualTo("foo");

        client.shutdown();
    }

    @Test
    public void shouldDeleteConfigObjectWhenCalledWithDeleteRequest() throws IOException {
        // Given
        AmazonS3 client = createClient();
        String bucketName = UUID.randomUUID().toString();
        client.createBucket(bucketName);
        client.putObject(bucketName, "config", "foo");

        // When
        InstanceProperties instanceProperties = createDefaultProperties("foo", bucketName);

        HashMap<String, Object> resourceProperties = new HashMap<>();
        resourceProperties.put("properties", instanceProperties.saveAsString());

        CloudFormationCustomResourceEvent event = CloudFormationCustomResourceEvent.builder()
                .withRequestType("Delete")
                .withResourceProperties(resourceProperties)
                .build();

        PropertiesWriterLambda lambda = new PropertiesWriterLambda(client);
        lambda.handleEvent(event, null);

        // Then
        assertThat(client.listObjects(bucketName).getObjectSummaries()).isEmpty();
        client.shutdown();
    }
}
