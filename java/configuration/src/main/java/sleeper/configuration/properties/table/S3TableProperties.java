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

package sleeper.configuration.properties.table;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.PropertiesUtils;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.table.TableId;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

public class S3TableProperties implements TablePropertiesStore.Client {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableProperties.class);

    private final InstanceProperties instanceProperties;
    private final AmazonS3 s3Client;

    private S3TableProperties(InstanceProperties instanceProperties, AmazonS3 s3Client) {
        this.instanceProperties = instanceProperties;
        this.s3Client = s3Client;
    }

    public static TablePropertiesStore getStore(
            InstanceProperties instanceProperties, AmazonS3 s3Client, AmazonDynamoDB dynamoClient) {
        return new TablePropertiesStore(
                new DynamoDBTableIndex(instanceProperties, dynamoClient),
                new S3TableProperties(instanceProperties, s3Client));
    }

    @Override
    public TableProperties loadProperties(TableId tableId) {
        String bucket = instanceProperties.get(CONFIG_BUCKET);
        String key = getS3Key(tableId);
        LOGGER.info("Loading table properties from bucket {}, key {}", bucket, key);
        String content = s3Client.getObjectAsString(bucket, key);
        return new TableProperties(instanceProperties, PropertiesUtils.loadProperties(content));
    }

    @Override
    public void saveProperties(TableProperties tableProperties) {
        String bucket = instanceProperties.get(CONFIG_BUCKET);
        String key = getS3Key(tableProperties.getId());
        s3Client.putObject(bucket, key, tableProperties.saveAsString());
        LOGGER.info("Saved table properties to bucket {}, key {}", bucket, key);
    }

    @Override
    public void deleteProperties(TableId tableId) {
        String bucket = instanceProperties.get(CONFIG_BUCKET);
        String key = getS3Key(tableId);
        s3Client.deleteObject(bucket, key);
        LOGGER.info("Deleted table properties in bucket {}, key {}", bucket, key);
    }

    private String getS3Key(TableId tableId) {
        return "tables/" + tableId.getTableUniqueId();
    }
}
