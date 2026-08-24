/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.api.resources;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * Shared teardown helpers for tests that run against a LocalStack container.
 */
final class LocalStackTestResources {

    private LocalStackTestResources() {
    }

    /**
     * Deletes all DynamoDB tables from LocalStack.
     *
     * @param dynamoDbClient the DynamoDB client
     */
    static void deleteDynamoTables(DynamoDbClient dynamoDbClient) {
        dynamoDbClient.listTables().tableNames()
                .forEach(name -> dynamoDbClient.deleteTable(builder -> builder.tableName(name)));
    }

    /**
     * Deletes all S3 buckets from LocalStack, including any objects they contain.
     *
     * @param s3Client the S3 client
     */
    static void deleteS3Buckets(S3Client s3Client) {
        s3Client.listBuckets().buckets().forEach(bucket -> {
            String name = bucket.name();
            s3Client.listObjectsV2Paginator(builder -> builder.bucket(name)).contents().forEach(obj -> s3Client
                    .deleteObject(builder -> builder.bucket(name).key(obj.key())));
            s3Client.deleteBucket(builder -> builder.bucket(name));
        });
    }

    /**
     * Deletes all SQS queues from LocalStack.
     *
     * @param sqsClient the SQS client
     */
    static void deleteSqsQueues(SqsClient sqsClient) {
        sqsClient.listQueues().queueUrls().forEach(url -> sqsClient.deleteQueue(builder -> builder.queueUrl(url)));
    }

}