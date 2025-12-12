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
package sleeper.spark;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

import static sleeper.core.properties.PropertiesUtils.loadProperties;

/**
 * Allows readers to be created for partitions of the Spark DataFrame.
 *
 * This class will be shipped to the Spark executors for execution, and hence needs to be Serializable.
 * For this reason, the instance and table properties are stored internally as {@link String}s.
 */
public class SleeperPartitionReaderFactory implements PartitionReaderFactory {
    private static final long serialVersionUID = 9223372036854775807L;

    private final String instancePropertiesAsString;
    private final String tablePropertiesAsString;

    public SleeperPartitionReaderFactory(String instancePropertiesAString, String tablePropertiesAsString) {
        this.instancePropertiesAsString = instancePropertiesAString;
        this.tablePropertiesAsString = tablePropertiesAsString;
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
        InstanceProperties instanceProperties = InstanceProperties.createWithoutValidation(loadProperties(instancePropertiesAsString));
        TableProperties tableProperties = new TableProperties(instanceProperties, loadProperties(tablePropertiesAsString));
        return new SleeperColumnarBatchPartitionReader(instanceProperties, tableProperties, partition);
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        InstanceProperties instanceProperties = InstanceProperties.createWithoutValidation(loadProperties(instancePropertiesAsString));
        TableProperties tableProperties = new TableProperties(instanceProperties, loadProperties(tablePropertiesAsString));
        return new SleeperRowPartitionReader(instanceProperties, tableProperties, partition);
    }

    @Override
    public boolean supportColumnarReads(InputPartition inputPartition) {
        return true;
    }
}
