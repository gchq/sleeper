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
package sleeper.configuration.properties.table;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.table.TableStatus;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static sleeper.configuration.properties.instance.CommonProperty.TABLE_PROPERTIES_PROVIDER_TIMEOUT_IN_MINS;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

/**
 * Caches Sleeper table properties to avoid repeated queries to the store. An instance of this class cannot be used
 * concurrently in multiple threads, as the cache is not thread-safe.
 */
public class TablePropertiesProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(TablePropertiesProvider.class);
    private final TablePropertiesStore propertiesStore;
    private final Duration cacheTimeout;
    private final Supplier<Instant> timeSupplier;
    private final Map<String, CacheEntry> cacheById = new HashMap<>();
    private final Map<String, CacheEntry> cacheByName = new HashMap<>();

    public TablePropertiesProvider(InstanceProperties instanceProperties, AmazonS3 s3Client, AmazonDynamoDB dynamoDBClient) {
        this(instanceProperties, S3TableProperties.getStore(instanceProperties, s3Client, dynamoDBClient));
    }

    public TablePropertiesProvider(InstanceProperties instanceProperties, TablePropertiesStore propertiesStore) {
        this(instanceProperties, propertiesStore, Instant::now);
    }

    public TablePropertiesProvider(InstanceProperties instanceProperties, TablePropertiesStore propertiesStore, Supplier<Instant> timeSupplier) {
        this(propertiesStore, Duration.ofMinutes(instanceProperties.getInt(TABLE_PROPERTIES_PROVIDER_TIMEOUT_IN_MINS)), timeSupplier);
    }

    protected TablePropertiesProvider(
            TablePropertiesStore propertiesStore, Duration cacheTimeout, Supplier<Instant> timeSupplier) {
        this.propertiesStore = propertiesStore;
        this.cacheTimeout = cacheTimeout;
        this.timeSupplier = timeSupplier;
    }

    /**
     * Retrieves properties by table name.
     *
     * @param  tableName the name of the table
     * @return           the table properties
     */
    public TableProperties getByName(String tableName) {
        return get(tableName, cacheByName, () -> propertiesStore.loadByName(tableName));
    }

    /**
     * Retrieves properties by table unique ID.
     *
     * @param  tableId the unique ID of the table
     * @return         the table properties
     */
    public TableProperties getById(String tableId) {
        return get(tableId, cacheById, () -> propertiesStore.loadById(tableId));
    }

    private TableProperties get(TableStatus table) {
        return get(table.getTableUniqueId(), cacheById, () -> propertiesStore.loadProperties(table));
    }

    private TableProperties get(
            String identifier,
            Map<String, CacheEntry> cache,
            Supplier<TableProperties> loadProperties) {
        Instant currentTime = timeSupplier.get();
        CacheEntry currentEntry = cache.get(identifier);
        TableProperties properties;
        if (currentEntry == null) {
            properties = loadProperties.get();
            LOGGER.info("Cache miss, loaded properties for table {}", properties.getStatus());
            cache(properties, currentTime);
        } else if (currentEntry.isExpired(currentTime)) {
            properties = loadProperties.get();
            LOGGER.info("Expiry time reached, reloaded properties for table {}", properties.getStatus());
            cache(properties, currentTime);
        } else {
            properties = currentEntry.getTableProperties();
            LOGGER.debug("Cache hit for table {}", properties.getStatus());
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

    /**
     * Retrieves properties of all tables in the Sleeper instance.
     *
     * @return the table properties
     */
    public Stream<TableProperties> streamAllTables() {
        return propertiesStore.streamAllTableStatuses()
                .map(this::get);
    }

    /**
     * Retrieves properties of all online tables in the Sleeper instance.
     *
     * @return the table properties
     */
    public Stream<TableProperties> streamOnlineTables() {
        return propertiesStore.streamOnlineTableIds()
                .map(this::get);
    }

    /**
     * Deletes all locally cached table properties. Further calls will reload properties.
     */
    public void clearCache() {
        cacheByName.clear();
        cacheById.clear();
    }

    /**
     * An entry in the provider cache for a given Sleeper table.
     */
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
}
