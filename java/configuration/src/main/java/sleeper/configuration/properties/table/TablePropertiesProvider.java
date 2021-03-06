/*
 * Copyright 2022 Crown Copyright
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
import sleeper.configuration.properties.InstanceProperties;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TablePropertiesProvider {
    private final AmazonS3 s3Client;
    private final InstanceProperties instanceProperties;
    private final Map<String, TableProperties> tableNameToPropertiesCache;

    public TablePropertiesProvider(AmazonS3 s3Client, InstanceProperties instanceProperties) {
        this.s3Client = s3Client;
        this.instanceProperties = instanceProperties;
        this.tableNameToPropertiesCache = new HashMap<>();
    }

    public TableProperties getTableProperties(String tableName) {
        if (!tableNameToPropertiesCache.containsKey(tableName)) {
            tableNameToPropertiesCache.put(tableName, getTablePropertiesFromS3(tableName));
        }
        return tableNameToPropertiesCache.get(tableName);
    }

    private TableProperties getTablePropertiesFromS3(String tableName) {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        try {
            tableProperties.loadFromS3(s3Client, tableName);
        } catch (IOException e) {
            throw new RuntimeException("Exception while trying to download table properties", e);
        }
        return tableProperties;
    }
}
