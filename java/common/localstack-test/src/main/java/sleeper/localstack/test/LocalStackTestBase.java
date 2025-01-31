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
package sleeper.localstack.test;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.util.List;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.localstack.test.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.localstack.test.LocalStackAwsV2ClientHelper.buildAwsV2Client;
import static sleeper.localstack.test.LocalStackHadoopConfigurationProvider.getHadoopConfiguration;

/**
 * A base class for tests to run against LocalStack.
 */
@Testcontainers
@SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
public abstract class LocalStackTestBase {

    private static final LocalStackContainer CONTAINER = SleeperLocalStackContainer.start(Service.S3, Service.DYNAMODB, Service.SQS);

    protected final AmazonS3 s3Client = buildAwsV1Client(CONTAINER, Service.S3, AmazonS3ClientBuilder.standard());
    protected final AmazonDynamoDB dynamoClient = buildAwsV1Client(CONTAINER, Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    protected final AmazonSQS sqsClient = buildAwsV1Client(CONTAINER, Service.SQS, AmazonSQSClientBuilder.standard());
    protected final S3Client s3ClientV2 = buildAwsV2Client(CONTAINER, Service.S3, S3Client.builder());
    protected final DynamoDbClient dynamoClientV2 = buildAwsV2Client(CONTAINER, Service.DYNAMODB, DynamoDbClient.builder());
    protected final SqsClient sqsClientV2 = buildAwsV2Client(CONTAINER, Service.SQS, SqsClient.builder());
    protected final Configuration hadoopConf = getHadoopConfiguration(CONTAINER);

    protected void createBucket(String bucketName) {
        s3ClientV2.createBucket(builder -> builder.bucket(bucketName));
    }

    protected PutObjectResponse putObject(String bucketName, String key, String content) {
        return s3ClientV2.putObject(builder -> builder.bucket(bucketName).key(key),
                RequestBody.fromString(content));
    }

    protected List<String> listObjectKeys(String bucketName) {
        return s3ClientV2.listObjectsV2Paginator(builder -> builder.bucket(bucketName))
                .contents().stream().map(S3Object::key)
                .collect(toUnmodifiableList());
    }

}
