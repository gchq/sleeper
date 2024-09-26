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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.s3.AmazonS3;

import sleeper.core.properties.instance.InstanceProperties;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.local.LoadLocalProperties.loadTablesFromInstancePropertiesFile;
import static sleeper.core.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

class NewInstanceValidator {
    private final AmazonS3 amazonS3;
    private final AmazonDynamoDB amazonDynamoDB;

    NewInstanceValidator(AmazonS3 amazonS3, AmazonDynamoDB amazonDynamoDB) {
        this.amazonS3 = amazonS3;
        this.amazonDynamoDB = amazonDynamoDB;
    }

    void validate(InstanceProperties instanceProperties, Path instancePropertyPath) {
        checkQueryResultsBucketDoesNotExist(instanceProperties);
        checkTableConfiguration(instanceProperties, instancePropertyPath);
    }

    private void checkQueryResultsBucketDoesNotExist(InstanceProperties instanceProperties) {
        String instanceName = instanceProperties.get(ID);
        String bucketName = String.join("-", "sleeper", instanceName, "query-results");

        if (amazonS3.doesBucketExistV2(bucketName)) {
            throw new IllegalArgumentException("Sleeper query results bucket exists: " + bucketName);
        }
    }

    private void checkTableConfiguration(InstanceProperties instanceProperties, Path instancePropertyPath) {
        String instanceName = instanceProperties.get(ID);

        loadTablesFromInstancePropertiesFile(instanceProperties, instancePropertyPath).forEach(tableProperties -> {
            String tableName = tableProperties.get(TABLE_NAME);

            checkBucketExistsForTable(instanceName, tableName);

            if ("sleeper.statestore.dynamodb.DynamoDBStateStore".equalsIgnoreCase(tableProperties.get(STATESTORE_CLASSNAME))) {
                checkDynamoDBConfigurationExistsForTable(instanceName, tableName);
            }
        });
    }

    private void checkDynamoDBConfigurationExistsForTable(String instanceName, String tableName) {
        List<String> tableTypes = Arrays.asList("active-files", "gc-files", "partitions");
        tableTypes.forEach(tableType -> {
            String dynamodbTableName = String.join("-", "sleeper", instanceName, "table", tableName, tableType);
            if (doesDynamoTableExist(dynamodbTableName)) {
                throw new IllegalArgumentException("Sleeper DynamoDBTable exists: " + dynamodbTableName);
            }
        });
    }

    private void checkBucketExistsForTable(String instanceName, String tableName) {
        String bucketName = String.join("-", "sleeper", instanceName, "table", tableName);
        if (amazonS3.doesBucketExistV2(bucketName)) {
            throw new IllegalArgumentException("Sleeper table bucket exists: " + bucketName);
        }
    }

    private boolean doesDynamoTableExist(String name) {
        boolean tableExists = true;
        try {
            amazonDynamoDB.describeTable(name);
        } catch (ResourceNotFoundException e) {
            tableExists = false;
        }
        return tableExists;
    }
}
