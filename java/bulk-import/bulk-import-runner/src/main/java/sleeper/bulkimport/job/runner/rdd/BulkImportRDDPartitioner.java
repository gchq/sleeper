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

package sleeper.bulkimport.job.runner.rdd;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.bulkimport.job.runner.BulkImportPartitioner;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;

import java.io.IOException;
import java.util.List;

import static sleeper.bulkimport.job.runner.BulkImportJobRunner.createFileInfoSchema;

public class BulkImportRDDPartitioner implements BulkImportPartitioner {
    @Override
    public Dataset<Row> createFileInfos(
            Dataset<Row> rows, BulkImportJob job, InstanceProperties instanceProperties, TableProperties tableProperties,
            Broadcast<List<Partition>> broadcastedPartitions, Configuration conf) throws IOException {
        Schema schema = tableProperties.getSchema();
        String schemaAsString = new SchemaSerDe().toJson(schema);
        JavaRDD<Row> rdd = rows.javaRDD()
                .mapToPair(new ExtractKeyFunction(schema.getRowKeyTypes().size() + schema.getSortKeyTypes().size())) // Sort by both row keys and sort keys
                .repartitionAndSortWithinPartitions(new SleeperPartitioner(schemaAsString, broadcastedPartitions), new WrappedKeyComparator(schemaAsString))
                .map(tuple -> tuple._2)
                .mapPartitions(new WriteParquetFile(instanceProperties.saveAsString(), tableProperties.saveAsString(), conf, broadcastedPartitions));

        SparkSession session = SparkSession.builder().getOrCreate();
        return session.createDataset(rdd.rdd(), RowEncoder.apply(createFileInfoSchema()));
    }
}
