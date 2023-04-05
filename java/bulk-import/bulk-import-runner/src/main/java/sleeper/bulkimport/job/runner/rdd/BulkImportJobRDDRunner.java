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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;

import sleeper.bulkimport.job.runner.BulkImportJobRunner;
import sleeper.bulkimport.job.runner.SparkFileInfoRow;
import sleeper.bulkimport.job.runner.SparkPartitionRequest;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;

import java.io.IOException;

/**
 * The {@link BulkImportJobRDDRunner} is a {@link BulkImportJobRunner} which
 * uses the Spark RDD API to partition the data according to the Sleeper
 * partitions and for each partition, write a single sorted Parquet file.
 */
public class BulkImportJobRDDRunner {
    private BulkImportJobRDDRunner() {
    }

    public static void main(String[] args) throws Exception {
        BulkImportJobRunner.start(args, BulkImportJobRDDRunner::createFileInfos);
    }

    public static Dataset<Row> createFileInfos(SparkPartitionRequest request) throws IOException {
        Schema schema = request.tableProperties().getSchema();
        String schemaAsString = new SchemaSerDe().toJson(schema);
        JavaRDD<Row> rdd = request.rows().javaRDD()
                .mapToPair(new ExtractKeyFunction(
                        schema.getRowKeyTypes().size() + schema.getSortKeyTypes().size())) // Sort by both row keys and sort keys
                .repartitionAndSortWithinPartitions(
                        new SleeperPartitioner(schemaAsString, request.broadcastedPartitions()),
                        new WrappedKeyComparator(schemaAsString))
                .map(tuple -> tuple._2)
                .mapPartitions(new WriteParquetFile(
                        request.instanceProperties().saveAsString(),
                        request.tableProperties().saveAsString(),
                        request.conf(), request.broadcastedPartitions()));

        SparkSession session = SparkSession.builder().getOrCreate();
        return session.createDataset(rdd.rdd(), RowEncoder.apply(SparkFileInfoRow.createFileInfoSchema()));
    }
}
