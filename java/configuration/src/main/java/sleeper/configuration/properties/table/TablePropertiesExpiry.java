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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.table.TableId;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class TablePropertiesExpiry {
    private static final Logger LOGGER = LoggerFactory.getLogger(TablePropertiesProvider.class);

    private final int timeoutInMins;
    private final Map<String, Instant> expiryByTableName = new HashMap<>();
    private final Map<String, Instant> expiryByTableId = new HashMap<>();

    public TablePropertiesExpiry(int timeoutInMins) {
        this.timeoutInMins = timeoutInMins;
    }

    public boolean isExpiredByName(String tableName, Instant currentTime) {
        if (expiryByTableName.containsKey(tableName) && currentTime.isAfter(expiryByTableName.get(tableName))) {
            LOGGER.info("Properties expiry time reached for table name {}", tableName);
            return true;
        }
        return false;
    }

    public boolean isExpiredById(String tableId, Instant currentTime) {
        if (expiryByTableId.containsKey(tableId) && currentTime.isAfter(expiryByTableId.get(tableId))) {
            LOGGER.info("Properties expiry time reached for table ID {}", tableId);
            return true;
        }
        return false;
    }

    public void setExpiryFromCurrentTime(TableId tableId, Instant currentTime) {
        Instant expiry = currentTime.plus(Duration.ofMinutes(timeoutInMins));
        LOGGER.info("Setting properties expiry time for table {}", tableId);
        expiryByTableName.put(tableId.getTableName(), expiry);
        expiryByTableId.put(tableId.getTableUniqueId(), expiry);
    }
}
