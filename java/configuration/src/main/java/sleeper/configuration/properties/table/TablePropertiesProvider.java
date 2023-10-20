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

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.table.TableId;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static sleeper.configuration.properties.instance.CommonProperty.TABLE_PROPERTIES_PROVIDER_TIMEOUT_IN_MINS;

public class TablePropertiesProvider {
    protected final TablePropertiesStore propertiesStore;
    private final TablePropertiesCache cache;
    private final TablePropertiesExpiry expiry;
    private final Supplier<Instant> timeSupplier;

    public TablePropertiesProvider(InstanceProperties instanceProperties, AmazonS3 s3Client, AmazonDynamoDB dynamoDBClient) {
        this(instanceProperties, s3Client, dynamoDBClient, Instant::now);
    }

    protected TablePropertiesProvider(InstanceProperties instanceProperties, AmazonS3 s3Client, AmazonDynamoDB dynamoDBClient, Supplier<Instant> timeSupplier) {
        this(instanceProperties, S3TableProperties.getStore(instanceProperties, s3Client, dynamoDBClient), timeSupplier);
    }

    public TablePropertiesProvider(InstanceProperties instanceProperties, TablePropertiesStore propertiesStore, Supplier<Instant> timeSupplier) {
        this(propertiesStore, instanceProperties.getInt(TABLE_PROPERTIES_PROVIDER_TIMEOUT_IN_MINS), timeSupplier);
    }

    protected TablePropertiesProvider(TablePropertiesStore propertiesStore,
                                      int timeoutInMins, Supplier<Instant> timeSupplier) {
        this.propertiesStore = propertiesStore;
        this.cache = new TablePropertiesCache();
        this.expiry = new TablePropertiesExpiry(timeoutInMins);
        this.timeSupplier = timeSupplier;
    }

    public TableProperties getByName(String tableName) {
        return get(new TableNameCacheRef(tableName));
    }

    public TableProperties getById(String tableId) {
        return get(new TableIdCacheRef(tableId));
    }

    public TableProperties get(TableId tableId) {
        return getById(tableId.getTableUniqueId());
    }

    private TableProperties get(CacheRef ref) {
        Instant currentTime = timeSupplier.get();
        if (ref.isExpired(currentTime)) {
            ref.remove();
        }
        return ref.getCached()
                .orElseGet(() -> {
                    TableProperties properties = ref.load().orElseThrow();
                    cache.add(properties);
                    expiry.setExpiryFromCurrentTime(properties.getId(), currentTime);
                    return properties;
                });
    }

    public Optional<TableId> lookupByName(String tableName) {
        return propertiesStore.lookupByName(tableName);
    }

    public Stream<TableId> streamAllTableIds() {
        return propertiesStore.streamAllTableIds();
    }

    public Stream<TableProperties> streamAllTables() {
        return propertiesStore.streamAllTableIds()
                .map(id -> getByName(id.getTableName()));
    }

    public List<String> listTableNames() {
        return propertiesStore.listTableNames();
    }

    public List<TableId> listTableIds() {
        return propertiesStore.listTableIds();
    }

    public void clearCache() {
        cache.clear();
    }

    interface CacheRef {
        boolean isExpired(Instant currentTime);


        void remove();

        Optional<TableProperties> getCached();

        Optional<TableProperties> load();
    }

    class TableNameCacheRef implements CacheRef {
        private final String tableName;

        public TableNameCacheRef(String tableName) {
            this.tableName = tableName;
        }

        @Override
        public boolean isExpired(Instant currentTime) {
            return expiry.isExpiredByName(tableName, currentTime);
        }

        @Override
        public void remove() {
            cache.removeByName(tableName);
        }

        @Override
        public Optional<TableProperties> getCached() {
            return cache.getByName(tableName);
        }

        @Override
        public Optional<TableProperties> load() {
            return propertiesStore.loadByName(tableName);
        }
    }

    class TableIdCacheRef implements CacheRef {
        private final String tableId;

        public TableIdCacheRef(String tableId) {
            this.tableId = tableId;
        }

        @Override
        public boolean isExpired(Instant currentTime) {
            return expiry.isExpiredById(tableId, currentTime);
        }

        @Override
        public void remove() {
            cache.removeById(tableId);
        }

        @Override
        public Optional<TableProperties> getCached() {
            return cache.getById(tableId);
        }

        @Override
        public Optional<TableProperties> load() {
            return propertiesStore.loadById(tableId);
        }
    }
}
