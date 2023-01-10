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
package sleeper.compaction.job;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.util.Collections;
import java.util.UUID;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class CompactionJobTestUtils {

    private CompactionJobTestUtils() {
    }

    public static final String KEY_FIELD = "key";

    public static Schema createSchema() {
        return Schema.builder().rowKeyFields(new Field(KEY_FIELD, new StringType())).build();
    }

    public static Partition singlePartition(Schema schema) {
        return new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct().get(0);
    }

    public static InstanceProperties createInstanceProperties() {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, UUID.randomUUID().toString());
        instanceProperties.set(CONFIG_BUCKET, "test-bucket");
        instanceProperties.set(FILE_SYSTEM, "test-fs");
        return instanceProperties;
    }

    public static TableProperties createTableProperties(Schema schema, InstanceProperties instanceProperties) {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, UUID.randomUUID().toString());
        tableProperties.setSchema(schema);
        return tableProperties;
    }
}
