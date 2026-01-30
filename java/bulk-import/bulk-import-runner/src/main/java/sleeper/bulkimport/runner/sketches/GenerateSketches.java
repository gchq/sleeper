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
package sleeper.bulkimport.runner.sketches;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

import java.util.Iterator;
import java.util.List;

import static sleeper.core.properties.PropertiesUtils.loadProperties;

public class GenerateSketches implements MapPartitionsFunction<Row, Row> {
    private static final long serialVersionUID = 1211201891202603297L;

    private final String instancePropertiesStr;
    private final String tablePropertiesStr;
    private final Broadcast<List<Partition>> broadcastPartitions;

    public GenerateSketches(InstanceProperties instanceProperties, TableProperties tableProperties, Broadcast<List<Partition>> broadcastPartitions) {
        this.instancePropertiesStr = instanceProperties.saveAsString();
        this.tablePropertiesStr = tableProperties.saveAsString();
        this.broadcastPartitions = broadcastPartitions;
    }

    @Override
    public SketchWritingterator call(Iterator<Row> input) throws Exception {
        InstanceProperties instanceProperties = InstanceProperties.createWithoutValidation(loadProperties(instancePropertiesStr));
        TableProperties tableProperties = new TableProperties(instanceProperties, loadProperties(tablePropertiesStr));
        PartitionTree partitionTree = new PartitionTree(broadcastPartitions.getValue());
        return new SketchWritingterator(input, tableProperties, partitionTree);
    }
}
