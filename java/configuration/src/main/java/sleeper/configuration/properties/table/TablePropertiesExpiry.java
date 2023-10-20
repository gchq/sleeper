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

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class TablePropertiesExpiry {
    private static final Logger LOGGER = LoggerFactory.getLogger(TablePropertiesProvider.class);

    private final int timeoutInMins;
    private final Supplier<Instant> timeSupplier;
    private final Map<String, Instant> expireTimeByTableName = new HashMap<>();

    public TablePropertiesExpiry(int timeoutInMins, Supplier<Instant> timeSupplier) {
        this.timeoutInMins = timeoutInMins;
        this.timeSupplier = timeSupplier;
    }

    public boolean isExpired(String tableName) {
        Instant currentTime = timeSupplier.get();
        if (expireTimeByTableName.containsKey(tableName) && currentTime.isAfter(expireTimeByTableName.get(tableName))) {
            LOGGER.info("Table properties provider expiry time reached for table {}, clearing cache.", tableName);
            return true;
        }
        expireTimeByTableName.put(tableName, currentTime.plus(Duration.ofMinutes(timeoutInMins)));
        return false;
    }
}
