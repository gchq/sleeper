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
package sleeper.cdk.testutils;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import sleeper.localstack.test.SleeperLocalStackContainer;

import java.util.List;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.localstack.test.LocalStackAwsV1ClientHelper.buildAwsV1Client;

@Testcontainers
public abstract class LocalStackTestBase {

    @Container
    public static LocalStackContainer localStackContainer = SleeperLocalStackContainer.create(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);

    protected final S3Client s3Client = buildAwsV2Client(localStackContainer, LocalStackContainer.Service.S3, S3Client.builder());
    protected final AmazonS3 s3ClientV1 = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    protected final DynamoDbClient dynamoClient = buildAwsV2Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, DynamoDbClient.builder());
    protected final AmazonDynamoDB dynamoClientV1 = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());

    private static <B extends AwsClientBuilder<B, T>, T> T buildAwsV2Client(LocalStackContainer localStackContainer, LocalStackContainer.Service service, B builder) {
        return builder
                .endpointOverride(localStackContainer.getEndpointOverride(service))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                        localStackContainer.getAccessKey(), localStackContainer.getSecretKey())))
                .region(Region.of(localStackContainer.getRegion()))
                .build();
    }

    protected void createBucket(String bucketName) {
        s3Client.createBucket(builder -> builder.bucket(bucketName));
    }

    protected PutObjectResponse putObject(String bucketName, String key, String content) {
        return s3Client.putObject(builder -> builder.bucket(bucketName).key(key),
                RequestBody.fromString(content));
    }

    protected List<String> listObjectKeys(String bucketName) {
        return s3Client.listObjectsV2Paginator(builder -> builder.bucket(bucketName))
                .contents().stream().map(S3Object::key)
                .collect(toUnmodifiableList());
    }
}
