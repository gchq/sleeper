/*
 * Copyright 2022-2025 Crown Copyright
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

import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;

import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Properties;

/**
 * Common properties for the Lambda Function.
 */
public class TableDefinerLambdaProperties {

    private InstanceProperties instanceProperties;
    private final TablePropertiesStore tablePropertiesStore;
    private final Map<String, Object> resourceProperties;
    private TableProperties tableProperties;

    public TableDefinerLambdaProperties(CloudFormationCustomResourceEvent event, S3Client s3Client, DynamoDbClient dynamoClient, String bucketName) {

        this.instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, bucketName);
        this.tablePropertiesStore = S3TableProperties.createStore(this.instanceProperties, s3Client, dynamoClient);
        this.resourceProperties = event.getResourceProperties();

        Properties properties = new Properties();
        try {
            properties.load(new StringReader((String) resourceProperties.get("tableProperties")));
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
        this.tableProperties = new TableProperties(instanceProperties, properties);
    }

    public InstanceProperties getInstanceProperties() {
        return instanceProperties;
    }

    public TablePropertiesStore getTablePropertiesStore() {
        return tablePropertiesStore;
    }

    public Map<String, Object> getResourceProperties() {
        return resourceProperties;
    }

    public TableProperties getTableProperties() {
        return tableProperties;
    }

    public void setTableProperties(TableProperties tableProperties) {
        this.tableProperties = tableProperties;
    }

    public void setInstanceProperties(InstanceProperties instanceProperties) {
        this.instanceProperties = instanceProperties;
    }

}
