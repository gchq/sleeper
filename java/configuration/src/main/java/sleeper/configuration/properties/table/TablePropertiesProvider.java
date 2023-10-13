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

import com.amazonaws.services.s3.AmazonS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static sleeper.configuration.properties.instance.CommonProperty.TABLE_PROPERTIES_PROVIDER_TIMEOUT_IN_MINS;

public class TablePropertiesProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(TablePropertiesProvider.class);
    private final Function<String, TableProperties> getTableProperties;
    private final Supplier<Instant> timeSupplier;
    private final int timeoutInMins;
    private final Map<String, TableProperties> tableNameToPropertiesCache = new HashMap<>();
    private final Map<String, Instant> expireTimeByTableName = new HashMap<>();

    public TablePropertiesProvider(AmazonS3 s3Client, InstanceProperties instanceProperties) {
        this(s3Client, instanceProperties, Instant::now);
    }

    protected TablePropertiesProvider(AmazonS3 s3Client, InstanceProperties instanceProperties, Supplier<Instant> timeSupplier) {
        this(tableName -> getTablePropertiesFromS3(s3Client, instanceProperties, tableName),
                instanceProperties.getInt(TABLE_PROPERTIES_PROVIDER_TIMEOUT_IN_MINS), timeSupplier);
    }

    protected TablePropertiesProvider(Function<String, TableProperties> getTableProperties,
                                      int timeoutInMins, Supplier<Instant> timeSupplier) {
        this.getTableProperties = getTableProperties;
        this.timeoutInMins = timeoutInMins;
        this.timeSupplier = timeSupplier;
    }

    public TableProperties getTableProperties(String tableName) {
        checkExpiryTime(tableName);
        return tableNameToPropertiesCache.computeIfAbsent(tableName, getTableProperties);
    }

    private void checkExpiryTime(String tableName) {
        Instant currentTime = timeSupplier.get();
        if (expireTimeByTableName.containsKey(tableName) && currentTime.isAfter(expireTimeByTableName.get(tableName))) {
            LOGGER.info("Table properties provider expiry time reached for table {}, clearing cache.", tableName);
            tableNameToPropertiesCache.remove(tableName);
        }
        expireTimeByTableName.put(tableName, currentTime.plus(Duration.ofMinutes(timeoutInMins)));
    }

    public Optional<TableProperties> getTablePropertiesIfExists(String tableName) {
        try {
            return Optional.of(getTableProperties(tableName));
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    private static TableProperties getTablePropertiesFromS3(
            AmazonS3 s3Client, InstanceProperties instanceProperties, String tableName) {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.loadFromS3(s3Client, tableName);
        return tableProperties;
    }

    public void clearCache() {
        tableNameToPropertiesCache.clear();
    }
}
