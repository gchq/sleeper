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
package sleeper.ingest.testutils;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import sleeper.core.CommonTestConstants;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.ingest.testutils.LocalStackAwsV2ClientHelper.buildAwsV2Client;

/**
 * This class is a JUnit extension which starts a local S3 and DynamoDB within a Docker
 * LocalStackContainer.
 * <p>
 * Only use one instance of this class at once if your code uses the Hadoop filing system, for example if you write
 * Parquet files to S3A. The Hadoop FileSystem caches the S3AFileSystem objects which actually communicate with S3 and
 * this means that any new localstack container will not be recognised once the first one has been used. The FileSystem
 * cache needs to be reset between different recreations of the localstack container, and this takes place in the {@link
 * #afterAll(ExtensionContext)} method.
 */
public class AwsExternalResource implements BeforeAllCallback, AfterAllCallback {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsExternalResource.class);

    private final LocalStackContainer localStackContainer;
    private final Set<LocalStackContainer.Service> localStackServiceSet;

    private AmazonS3 s3Client;
    private S3AsyncClient s3AsyncClient;
    private AmazonDynamoDB dynamoDBClient;
    private AmazonSQS sqsClient;
    private AmazonCloudWatch cloudWatchClient;

    public AwsExternalResource(LocalStackContainer.Service... services) {
        this.localStackContainer =
                new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
                        .withServices(services)
                        .withLogConsumer(outputFrame -> LOGGER.info("LocalStack log: " + outputFrame.getUtf8String()))
                        .withEnv("DEBUG", "1");
        this.localStackServiceSet = Arrays.stream(services).collect(Collectors.toSet());
    }

    private AmazonS3 createS3Client() {
        return buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    }

    private AmazonSQS createSQSClient() {
        return buildAwsV1Client(localStackContainer, LocalStackContainer.Service.SQS, AmazonSQSClientBuilder.standard());
    }

    private AmazonDynamoDB createDynamoClient() {
        return buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    }

    private S3AsyncClient createS3AsyncClient() {
        return buildAwsV2Client(localStackContainer, LocalStackContainer.Service.S3, S3AsyncClient.builder());
    }

    private AmazonCloudWatch createCloudWatchClient() {
        return buildAwsV1Client(localStackContainer, LocalStackContainer.Service.CLOUDWATCH, AmazonCloudWatchClientBuilder.standard());
    }

    @Override
    public void beforeAll(ExtensionContext context) {
        localStackContainer.start();
        s3Client = (localStackServiceSet.contains(LocalStackContainer.Service.S3)) ? createS3Client() : null;
        s3AsyncClient = (localStackServiceSet.contains(LocalStackContainer.Service.S3)) ? createS3AsyncClient() : null;
        dynamoDBClient = (localStackServiceSet.contains(LocalStackContainer.Service.DYNAMODB)) ? createDynamoClient() : null;
        sqsClient = (localStackServiceSet.contains(LocalStackContainer.Service.SQS)) ? createSQSClient() : null;
        cloudWatchClient = (localStackServiceSet.contains(LocalStackContainer.Service.CLOUDWATCH)) ? createCloudWatchClient() : null;

        LOGGER.info("S3 endpoint:         {}", localStackContainer.getEndpointOverride(LocalStackContainer.Service.S3).toString());
        LOGGER.info("DynamoDB endpoint:   {}", localStackContainer.getEndpointOverride(LocalStackContainer.Service.DYNAMODB).toString());
        LOGGER.info("SQS endpoint:        {}", localStackContainer.getEndpointOverride(LocalStackContainer.Service.SQS).toString());
        LOGGER.info("CloudWatch endpoint: {}", localStackContainer.getEndpointOverride(LocalStackContainer.Service.CLOUDWATCH).toString());
    }

    public void clear() {
        if (localStackServiceSet.contains(LocalStackContainer.Service.S3)) {
            s3Client.listBuckets().forEach(bucket -> {
                boolean objectsRemain = true;
                ObjectListing objectListing = s3Client.listObjects(bucket.getName());
                while (objectsRemain) {
                    for (S3ObjectSummary s3ObjectSummary : objectListing.getObjectSummaries()) {
                        s3Client.deleteObject(bucket.getName(), s3ObjectSummary.getKey());
                    }
                    objectsRemain = objectListing.isTruncated();
                    if (objectsRemain) {
                        objectListing = s3Client.listNextBatchOfObjects(objectListing);
                    }
                }
                s3Client.deleteBucket(bucket.getName());
            });
            if (s3Client.listBuckets().size() > 0) {
                throw new AssertionError("Clearing S3 failed");
            }
        }
        if (localStackServiceSet.contains(LocalStackContainer.Service.DYNAMODB)) {
            dynamoDBClient.listTables().getTableNames().forEach(dynamoDBClient::deleteTable);
            while (dynamoDBClient.listTables().getTableNames().size() > 0) {
                LOGGER.info("Waiting for {} tables to be deleted", dynamoDBClient.listTables().getTableNames().size());
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        if (localStackServiceSet.contains(LocalStackContainer.Service.SQS)) {
            sqsClient.listQueues().getQueueUrls().forEach(sqsClient::deleteQueue);
        }

        // The Hadoop file system maintains a cache of the file system object to use. The S3AFileSystem object
        // retains the endpoint URL and so the cache needs to be cleared whenever the localstack instance changes.
        try {
            FileSystem.closeAll();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void afterAll(ExtensionContext context) {
        clear();
        if (localStackServiceSet.contains(LocalStackContainer.Service.S3)) {
            s3Client.shutdown();
            s3AsyncClient.close();
        }
        if (localStackServiceSet.contains(LocalStackContainer.Service.DYNAMODB)) {
            dynamoDBClient.shutdown();
        }
        if (localStackServiceSet.contains(LocalStackContainer.Service.SQS)) {
            sqsClient.shutdown();
        }
        if (localStackServiceSet.contains(LocalStackContainer.Service.CLOUDWATCH)) {
            cloudWatchClient.shutdown();
        }
        localStackContainer.stop();
    }

    public AmazonS3 getS3Client() {
        if (localStackServiceSet.contains(LocalStackContainer.Service.S3)) {
            return s3Client;
        }
        throw new AssertionError("Localstack instance was not created with S3 support");
    }

    public S3AsyncClient getS3AsyncClient() {
        if (localStackServiceSet.contains(LocalStackContainer.Service.S3)) {
            return s3AsyncClient;
        }
        throw new AssertionError("Localstack instance was not created with S3 support");
    }

    public AmazonDynamoDB getDynamoDBClient() {
        if (localStackServiceSet.contains(LocalStackContainer.Service.DYNAMODB)) {
            return dynamoDBClient;
        }
        throw new AssertionError("Localstack instance was not created with DynamoDB support");
    }

    public AmazonSQS getSqsClient() {
        if (localStackServiceSet.contains(LocalStackContainer.Service.SQS)) {
            return sqsClient;
        }
        throw new AssertionError("Localstack instance was not created with SQS support");
    }

    public AmazonCloudWatch getCloudWatchClient() {
        if (localStackServiceSet.contains(LocalStackContainer.Service.CLOUDWATCH)) {
            return cloudWatchClient;
        }
        throw new AssertionError("Localstack instance was not created with CloudWatch support");
    }

    public Configuration getHadoopConfiguration() {
        if (localStackServiceSet.contains(LocalStackContainer.Service.S3)) {
            Configuration configuration = new Configuration();
            configuration.setClassLoader(this.getClass().getClassLoader());
            configuration.set("fs.s3a.endpoint", localStackContainer.getEndpointOverride(LocalStackContainer.Service.S3).toString());
            configuration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
            configuration.set("fs.s3a.access.key", localStackContainer.getAccessKey());
            configuration.set("fs.s3a.secret.key", localStackContainer.getSecretKey());
            configuration.setInt("fs.s3a.connection.maximum", 25);
            configuration.setBoolean("fs.s3a.connection.ssl.enabled", false);
            // The following settings may be useful if the connection to the localstack S3 instance hangs.
            // These settings attempt to force connection issues to generate errors ealy.
            // The settings do help but errors mayn still take many minutes to appear.
            // configuration.set("fs.s3a.connection.timeout", "1000");
            // configuration.set("fs.s3a.connection.establish.timeout", "1");
            // configuration.set("fs.s3a.attempts.maximum", "1");
            return configuration;
        }
        throw new AssertionError("Localstack instance was not created with S3 support");
    }
}
