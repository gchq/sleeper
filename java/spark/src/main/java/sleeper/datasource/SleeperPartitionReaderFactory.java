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
package sleeper.datasource;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

/**
 * Needs to be serializable.
 */
public class SleeperPartitionReaderFactory implements PartitionReaderFactory {
    private String instancePropertiesAsString;
    private String tablePropertiesAsString;

    public SleeperPartitionReaderFactory(String instancePropertiesAString, String tablePropertiesAsString) {
        this.instancePropertiesAsString = instancePropertiesAString;
        this.tablePropertiesAsString = tablePropertiesAsString;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        InstanceProperties instanceProperties = Utils.loadInstancePropertiesFromString(instancePropertiesAsString);
        TableProperties tableProperties = Utils.loadTablePropertiesFromString(instanceProperties, tablePropertiesAsString);
        return new SleeperPartitionReader(instanceProperties, tableProperties, partition);
    }
}