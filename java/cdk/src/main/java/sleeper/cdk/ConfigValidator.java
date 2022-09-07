/*
 * Copyright 2022 Crown Copyright
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
package sleeper.cdk;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.internal.BucketNameUtils;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.TABLE_PROPERTIES;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class ConfigValidator {
    private final AmazonS3 amazonS3;
    private final AmazonDynamoDB amazonDynamoDB;

    public ConfigValidator(AmazonS3 amazonS3, AmazonDynamoDB amazonDynamoDB) {
        this.amazonS3 = amazonS3;
        this.amazonDynamoDB = amazonDynamoDB;
    }

    public void validate(InstanceProperties instanceProperties) {
        checkForValidInstanceId(instanceProperties);
        checkQueryResultsBucketDoesNotExist(instanceProperties);
        checkTableConfiguration(instanceProperties);
    }

    private void checkQueryResultsBucketDoesNotExist(InstanceProperties instanceProperties) {
        String instanceName = instanceProperties.get(ID);
        String bucketName = String.join("-", "sleeper", instanceName, "query-results");

        if (amazonS3.doesBucketExistV2(bucketName)) {
            throw new IllegalArgumentException("Sleeper query results bucket exists: " + bucketName);
        }
    }

    private void checkForValidInstanceId(InstanceProperties instanceProperties) {
        if (!BucketNameUtils.isValidV2BucketName(instanceProperties.get(ID))) {
            throw new IllegalArgumentException("Sleeper instance id is illegal: " + instanceProperties.get(ID));
        }
    }

    private void checkTableConfiguration(InstanceProperties instanceProperties) {
        String instanceName = instanceProperties.get(ID);

        if (instanceProperties.getList(TABLE_PROPERTIES) != null) {
            for (String propertiesFilePath : instanceProperties.getList(TABLE_PROPERTIES)) {
                try {
                    TableProperties tableProperties = new TableProperties(instanceProperties);
                    tableProperties.load(new FileInputStream(propertiesFilePath));
                    String tableName = tableProperties.get(TABLE_NAME);

                    checkBucketConfigurationForTable(instanceName, tableName);

                    if ("sleeper.statestore.dynamodb.DynamoDBStateStore".equalsIgnoreCase(tableProperties.get(STATESTORE_CLASSNAME))) {
                        checkDynamoDBConfigurationForTable(instanceName, tableName);
                    }
                } catch (FileNotFoundException ep) {
                    throw new IllegalArgumentException("Could not find table properties file");
                }
            }
        }
    }

    private void checkDynamoDBConfigurationForTable(String instanceName, String tableName) {
        List<String> tableTypes = Arrays.asList("active-files", "gc-files", "partitions");
        tableTypes.stream().forEach(tableType -> {
            String dynamodbTableName = String.join("-", "sleeper", instanceName, "table", tableName, tableType);
            if (doesDynamoTableExist(dynamodbTableName)) {
                throw new IllegalArgumentException("Sleeper DynamoDBTable exists: " + dynamodbTableName);
            }
        });
    }

    private void checkBucketConfigurationForTable(String instanceName, String tableName) {
        String bucketName = String.join("-", "sleeper", instanceName, "table", tableName);
        if (!BucketNameUtils.isValidV2BucketName(bucketName)) {
            throw new IllegalArgumentException("Sleeper table bucket name is illegal: " + bucketName);
        }
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
