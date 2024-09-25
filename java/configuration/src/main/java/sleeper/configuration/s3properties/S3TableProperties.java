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

package sleeper.configuration.s3properties;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TablePropertiesStore;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.PropertiesUtils;
import sleeper.core.table.TableNotFoundException;
import sleeper.core.table.TableStatus;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * Saves and loads table properties in AWS S3.
 */
public class S3TableProperties implements TablePropertiesStore.Client {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableProperties.class);

    private final InstanceProperties instanceProperties;
    private final AmazonS3 s3Client;

    private S3TableProperties(InstanceProperties instanceProperties, AmazonS3 s3Client) {
        this.instanceProperties = instanceProperties;
        this.s3Client = s3Client;
    }

    /**
     * Creates a store for loading and saving table properties in S3, via the table index.
     *
     * @param  instanceProperties the instance properties
     * @param  s3Client           the S3 client
     * @param  dynamoClient       the DynamoDB client
     * @return                    the store
     */
    public static TablePropertiesStore getStore(
            InstanceProperties instanceProperties, AmazonS3 s3Client, AmazonDynamoDB dynamoClient) {
        return new TablePropertiesStore(
                new DynamoDBTableIndex(instanceProperties, dynamoClient),
                new S3TableProperties(instanceProperties, s3Client));
    }

    /**
     * Creates a provider for loading and caching table properties from S3, via the table index.
     *
     * @param  instanceProperties the instance properties
     * @param  s3Client           the S3 client
     * @param  dynamoClient       the DynamoDB client
     * @return                    the store
     */
    public static TablePropertiesProvider createProvider(
            InstanceProperties instanceProperties, AmazonS3 s3Client, AmazonDynamoDB dynamoClient) {
        return new TablePropertiesProvider(instanceProperties, getStore(instanceProperties, s3Client, dynamoClient));
    }

    @Override
    public TableProperties loadProperties(TableStatus table) {
        String bucket = instanceProperties.get(CONFIG_BUCKET);
        String key = getS3Key(table);
        LOGGER.info("Loading table properties from bucket {}, key {}", bucket, key);
        try {
            String content = s3Client.getObjectAsString(bucket, key);
            return new TableProperties(instanceProperties, PropertiesUtils.loadProperties(content));
        } catch (AmazonS3Exception e) {
            throw TableNotFoundException.withTable(table, e);
        }
    }

    @Override
    public void saveProperties(TableProperties tableProperties) {
        String bucket = instanceProperties.get(CONFIG_BUCKET);
        String key = getS3Key(tableProperties.getStatus());
        s3Client.putObject(bucket, key, tableProperties.saveAsString());
        LOGGER.info("Saved table properties to bucket {}, key {}", bucket, key);
    }

    @Override
    public void deleteProperties(TableStatus table) {
        String bucket = instanceProperties.get(CONFIG_BUCKET);
        String key = getS3Key(table);
        s3Client.deleteObject(bucket, key);
        LOGGER.info("Deleted table properties in bucket {}, key {}", bucket, key);
    }

    private String getS3Key(TableStatus table) {
        return "tables/table-" + table.getTableUniqueId() + ".properties";
    }
}
