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

package sleeper.configurationv2.properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;

import sleeper.configurationv2.table.index.DynamoDBTableIndex;
import sleeper.core.properties.PropertiesUtils;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableNotFoundException;
import sleeper.core.table.TableStatus;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * Saves and loads table properties in AWS S3.
 */
public class S3TableProperties implements TablePropertiesStore.Client {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableProperties.class);

    private final InstanceProperties instanceProperties;
    private final S3Client s3Client;

    private S3TableProperties(InstanceProperties instanceProperties, S3Client s3Client) {
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
    public static TablePropertiesStore createStore(
            InstanceProperties instanceProperties, S3Client s3Client, DynamoDbClient dynamoClient) {
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
            InstanceProperties instanceProperties, S3Client s3Client, DynamoDbClient dynamoClient) {
        return new TablePropertiesProvider(instanceProperties, createStore(instanceProperties, s3Client, dynamoClient));
    }

    /**
     * Creates a provider for loading and caching table properties from S3, via the table index.
     *
     * @param  instanceProperties the instance properties
     * @param  tableIndex         the index of Sleeper tables
     * @param  s3Client           the S3 client
     * @return                    the store
     */
    public static TablePropertiesProvider createProvider(
            InstanceProperties instanceProperties, TableIndex tableIndex, S3Client s3Client) {
        return new TablePropertiesProvider(instanceProperties,
                new TablePropertiesStore(tableIndex, new S3TableProperties(instanceProperties, s3Client)));
    }

    @Override
    public TableProperties loadProperties(TableStatus table) {
        String bucket = instanceProperties.get(CONFIG_BUCKET);
        String key = getS3Key(table);
        LOGGER.info("Loading table properties from bucket {}, key {}", bucket, key);
        try {
            String content = s3Client.getObjectAsBytes(builder -> builder.bucket(bucket)
                    .key(key)).asUtf8String();

            return new TableProperties(instanceProperties, PropertiesUtils.loadProperties(content));
        } catch (S3Exception e) {
            throw TableNotFoundException.withTable(table, e);
        }
    }

    @Override
    public void saveProperties(TableProperties tableProperties) {
        String bucket = instanceProperties.get(CONFIG_BUCKET);
        String key = getS3Key(tableProperties.getStatus());
        s3Client.putObject(builder -> builder.bucket(bucket).key(key),
                RequestBody.fromString(tableProperties.saveAsString()));

        LOGGER.info("Saved table properties to bucket {}, key {}", bucket, key);
    }

    @Override
    public void deleteProperties(TableStatus table) {
        String bucket = instanceProperties.get(CONFIG_BUCKET);
        String key = getS3Key(table);
        s3Client.deleteObject(builder -> builder.bucket(bucket).key(key));
        LOGGER.info("Deleted table properties in bucket {}, key {}", bucket, key);
    }

    private String getS3Key(TableStatus table) {
        return "tables/table-" + table.getTableUniqueId() + ".properties";
    }
}
