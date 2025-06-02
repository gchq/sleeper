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
package sleeper.bulkimport.runner.rdd;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.util.SerializableConfiguration;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.sketchesv2.store.S3SketchesStore;

import java.util.Iterator;
import java.util.List;

import static sleeper.core.properties.PropertiesUtils.loadProperties;

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

        try (S3Client s3Client = S3Client.create();
                S3AsyncClient s3AsyncClient = S3AsyncClient.crtCreate();
                S3TransferManager s3TransferManager = S3TransferManager.builder().s3Client(s3AsyncClient).build()) {
            return new SingleFileWritingIterator(rowIter, instanceProperties, tableProperties, serializableConf.value(), new S3SketchesStore(s3Client, s3TransferManager), partitionTree);
        }
    }

}
