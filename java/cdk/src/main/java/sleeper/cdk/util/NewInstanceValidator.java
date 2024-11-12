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

package sleeper.cdk.util;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

import sleeper.core.properties.instance.InstanceProperties;

import java.nio.file.Path;

import static sleeper.core.properties.instance.CommonProperty.ID;

class NewInstanceValidator {
    private final S3Client s3Client;
    private final DynamoDbClient dynamoClient;

    NewInstanceValidator(S3Client s3Client, DynamoDbClient dynamoClient) {
        this.s3Client = s3Client;
        this.dynamoClient = dynamoClient;
    }

    void validate(InstanceProperties instanceProperties, Path instancePropertyPath) {
        checkQueryResultsBucketDoesNotExist(instanceProperties);
        checkDataBucketDoesNotExist(instanceProperties);
        checkDynamoDBStateStoreDoesNotExist(instanceProperties);
        checkS3StateStoreDoesNotExist(instanceProperties);
        checkTransactionLogStateStoreDoesNotExist(instanceProperties);
    }

    private void checkQueryResultsBucketDoesNotExist(InstanceProperties instanceProperties) {
        String bucketName = String.join("-", "sleeper", instanceProperties.get(ID), "query", "results");
        if (doesBucketExist(bucketName)) {
            throw new IllegalArgumentException("Sleeper query results bucket exists: " + bucketName);
        }
    }

    private void checkDataBucketDoesNotExist(InstanceProperties instanceProperties) {
        String bucketName = String.join("-", "sleeper", instanceProperties.get(ID), "table", "data");
        if (doesBucketExist(bucketName)) {
            throw new IllegalArgumentException("Sleeper data bucket exists: " + bucketName);
        }
    }

    private void checkDynamoDBStateStoreDoesNotExist(InstanceProperties instanceProperties) {
        String dynamodbTableName = String.join("-", "sleeper", instanceProperties.get(ID), "partitions");
        if (doesDynamoTableExist(dynamodbTableName)) {
            throw new IllegalArgumentException("Sleeper state store table exists: " + dynamodbTableName);
        }
    }

    private void checkS3StateStoreDoesNotExist(InstanceProperties instanceProperties) {
        String dynamodbTableName = String.join("-", "sleeper", instanceProperties.get(ID), "table", "revisions");
        if (doesDynamoTableExist(dynamodbTableName)) {
            throw new IllegalArgumentException("Sleeper state store table exists: " + dynamodbTableName);
        }
    }

    private void checkTransactionLogStateStoreDoesNotExist(InstanceProperties instanceProperties) {
        String dynamodbTableName = String.join("-", "sleeper", instanceProperties.get(ID), "partition", "transaction", "log");
        if (doesDynamoTableExist(dynamodbTableName)) {
            throw new IllegalArgumentException("Sleeper state store table exists: " + dynamodbTableName);
        }
    }

    private boolean doesBucketExist(String bucketName) {
        try {
            s3Client.headBucket(builder -> builder.bucket(bucketName));
            return true;
        } catch (NoSuchBucketException e) {
            return false;
        }
    }

    private boolean doesDynamoTableExist(String name) {
        boolean tableExists = true;

        try {
            dynamoClient.describeTable(builder -> builder.tableName(name));
        } catch (ResourceNotFoundException e) {
            tableExists = false;
        }
        return tableExists;
    }
}
