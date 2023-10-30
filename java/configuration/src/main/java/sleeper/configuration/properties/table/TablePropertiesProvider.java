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

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.table.TableIdentity;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static sleeper.configuration.properties.instance.CommonProperty.TABLE_PROPERTIES_PROVIDER_TIMEOUT_IN_MINS;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class TablePropertiesProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(TablePropertiesProvider.class);
    private final TablePropertiesStore propertiesStore;
    private final Duration cacheTimeout;
    private final Supplier<Instant> timeSupplier;
    private final Map<String, CacheEntry> cacheById = new HashMap<>();
    private final Map<String, CacheEntry> cacheByName = new HashMap<>();

    public TablePropertiesProvider(InstanceProperties instanceProperties, AmazonS3 s3Client, AmazonDynamoDB dynamoDBClient) {
        this(instanceProperties, s3Client, dynamoDBClient, Instant::now);
    }

    protected TablePropertiesProvider(InstanceProperties instanceProperties, AmazonS3 s3Client, AmazonDynamoDB dynamoDBClient, Supplier<Instant> timeSupplier) {
        this(instanceProperties, S3TableProperties.getStore(instanceProperties, s3Client, dynamoDBClient), timeSupplier);
    }

    public TablePropertiesProvider(InstanceProperties instanceProperties, TablePropertiesStore propertiesStore, Supplier<Instant> timeSupplier) {
        this(propertiesStore, Duration.ofMinutes(instanceProperties.getInt(TABLE_PROPERTIES_PROVIDER_TIMEOUT_IN_MINS)), timeSupplier);
    }

    protected TablePropertiesProvider(TablePropertiesStore propertiesStore,
                                      Duration cacheTimeout, Supplier<Instant> timeSupplier) {
        this.propertiesStore = propertiesStore;
        this.cacheTimeout = cacheTimeout;
        this.timeSupplier = timeSupplier;
    }

    public TableProperties getByName(String tableName) {
        return get(tableName, cacheByName, () -> propertiesStore.loadByName(tableName)
                .orElseThrow(() -> new TableNotFoundException(tableName)));
    }

    public TableProperties getById(String tableId) {
        return get(tableId, cacheById, () -> propertiesStore.loadById(tableId).orElseThrow());
    }

    public TableProperties get(TableIdentity tableId) {
        return get(tableId.getTableUniqueId(), cacheById, () -> propertiesStore.loadProperties(tableId));
    }

    private TableProperties get(String identifier,
                                Map<String, CacheEntry> cache,
                                Supplier<TableProperties> loadProperties) {
        Instant currentTime = timeSupplier.get();
        CacheEntry currentEntry = cache.get(identifier);
        TableProperties properties;
        if (currentEntry == null) {
            properties = loadProperties.get();
            LOGGER.info("Cache miss, loaded properties for table {}", properties.getId());
            cache(properties, currentTime);
        } else if (currentEntry.isExpired(currentTime)) {
            properties = loadProperties.get();
            LOGGER.info("Expiry time reached, reloaded properties for table {}", properties.getId());
            cache(properties, currentTime);
        } else {
            properties = currentEntry.getTableProperties();
            LOGGER.info("Cache hit for table {}", properties.getId());
        }
        return properties;
    }

    private void cache(TableProperties properties, Instant currentTime) {
        Instant expiryTime = currentTime.plus(cacheTimeout);
        LOGGER.info("Setting expiry time: {}", expiryTime);
        CacheEntry entry = new CacheEntry(properties, expiryTime);
        cacheById.put(properties.get(TABLE_ID), entry);
        cacheByName.put(properties.get(TABLE_NAME), entry);
    }

    public Stream<TableProperties> streamAllTables() {
        return propertiesStore.streamAllTableIds()
                .map(id -> getByName(id.getTableName()));
    }

    public List<String> listTableNames() {
        return propertiesStore.listTableNames();
    }

    public List<TableIdentity> listTableIds() {
        return propertiesStore.listTableIds();
    }

    public void clearCache() {
        cacheByName.clear();
        cacheById.clear();
    }

    private static class CacheEntry {
        private final TableProperties tableProperties;
        private final Instant expiryTime;

        CacheEntry(TableProperties tableProperties, Instant expiryTime) {
            this.tableProperties = tableProperties;
            this.expiryTime = expiryTime;
        }

        TableProperties getTableProperties() {
            return tableProperties;
        }

        boolean isExpired(Instant currentTime) {
            return currentTime.isAfter(expiryTime);
        }
    }

    public static class TableNotFoundException extends RuntimeException {
        public TableNotFoundException(String tableName) {
            super("Table with name \"" + tableName + "\" not found");
        }
    }
}
