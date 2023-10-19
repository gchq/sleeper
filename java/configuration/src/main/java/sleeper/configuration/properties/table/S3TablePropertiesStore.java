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

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.table.TableId;

public class S3TablePropertiesStore implements TablePropertiesStore {

    private final InstanceProperties instanceProperties;
    private final AmazonS3 s3Client;

    public S3TablePropertiesStore(InstanceProperties instanceProperties, AmazonS3 s3Client) {
        this.instanceProperties = instanceProperties;
        this.s3Client = s3Client;
    }

    @Override
    public TableProperties loadProperties(TableId tableId) {
        TableProperties properties = new TableProperties(instanceProperties);
        properties.loadFromS3(s3Client, tableId.getTableName());
        return properties;
    }

    @Override
    public void save(TableProperties tableProperties) {
        tableProperties.saveToS3(s3Client);
    }
}
