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
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import static sleeper.localstack.test.LocalStackAwsV1ClientHelper.buildAwsV1Client;

/**
 * A base class for tests to run against LocalStack.
 */
@Testcontainers
public abstract class LocalStackTestBase {

    private static final LocalStackContainer CONTAINER = SleeperLocalStackContainer.start(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);

    protected final AmazonS3 s3Client = buildAwsV1Client(CONTAINER, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    protected final AmazonDynamoDB dynamoDBClient = buildAwsV1Client(CONTAINER, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    protected final AmazonSQS sqsClient = buildAwsV1Client(CONTAINER, LocalStackContainer.Service.SQS, AmazonSQSClientBuilder.standard());

}
