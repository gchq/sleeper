/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.bulkimport.job.runner.rdd;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.util.SerializableConfiguration;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;

import java.util.Iterator;
import java.util.List;

import static sleeper.configuration.properties.PropertiesUtils.loadProperties;

/**
 * Writes sorted rows to a Parquet file.
 */
public class WriteParquetFile implements FlatMapFunction<Iterator<Row>, Row>, MapPartitionsFunction<Row, Row> {
    private static final long serialVersionUID = 1873341639622053831L;

    private final String instancePropertiesStr;
    private final String tablePropertiesStr;
    private final SerializableConfiguration serializableConf;
    private final Broadcast<List<Partition>> broadcastPartitions;

    public WriteParquetFile(String instancePropertiesStr, String tablePropertiesStr, Configuration conf, Broadcast<List<Partition>> broadcastPartitions) {
        this.instancePropertiesStr = instancePropertiesStr;
        this.tablePropertiesStr = tablePropertiesStr;
        this.serializableConf = new SerializableConfiguration(conf);
        this.broadcastPartitions = broadcastPartitions;
    }

    @Override
    public Iterator<Row> call(Iterator<Row> rowIter) {
        InstanceProperties instanceProperties = InstanceProperties.createWithoutValidation(loadProperties(instancePropertiesStr));
        TableProperties tableProperties = new TableProperties(instanceProperties, loadProperties(tablePropertiesStr));

        PartitionTree partitionTree = new PartitionTree(broadcastPartitions.getValue());

        return new SingleFileWritingIterator(rowIter, instanceProperties, tableProperties, serializableConf.value(), partitionTree);
    }
}
