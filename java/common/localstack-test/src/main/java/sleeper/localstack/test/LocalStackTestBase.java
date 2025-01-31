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
@SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
public abstract class LocalStackTestBase {

    public static final LocalStackContainer CONTAINER = SleeperLocalStackContainer.start(Service.S3, Service.DYNAMODB, Service.SQS);

    public static final AmazonS3 S3_CLIENT = buildAwsV1Client(CONTAINER, Service.S3, AmazonS3ClientBuilder.standard());
    public static final AmazonDynamoDB DYNAMO_CLIENT = buildAwsV1Client(CONTAINER, Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    public static final AmazonSQS SQS_CLIENT = buildAwsV1Client(CONTAINER, Service.SQS, AmazonSQSClientBuilder.standard());
    public static final S3Client S3_CLIENT_V2 = buildAwsV2Client(CONTAINER, Service.S3, S3Client.builder());
    public static final DynamoDbClient DYNAMO_CLIENT_V2 = buildAwsV2Client(CONTAINER, Service.DYNAMODB, DynamoDbClient.builder());
    public static final SqsClient SQS_CLIENT_V2 = buildAwsV2Client(CONTAINER, Service.SQS, SqsClient.builder());
    public static final Configuration HADOOP_CONF = getHadoopConfiguration(CONTAINER);

    protected static void createBucket(String bucketName) {
        S3_CLIENT_V2.createBucket(builder -> builder.bucket(bucketName));
    }

    protected static PutObjectResponse putObject(String bucketName, String key, String content) {
        return S3_CLIENT_V2.putObject(builder -> builder.bucket(bucketName).key(key),
                RequestBody.fromString(content));
    }

    protected static List<String> listObjectKeys(String bucketName) {
        return S3_CLIENT_V2.listObjectsV2Paginator(builder -> builder.bucket(bucketName))
                .contents().stream().map(S3Object::key)
                .collect(toUnmodifiableList());
    }

}
