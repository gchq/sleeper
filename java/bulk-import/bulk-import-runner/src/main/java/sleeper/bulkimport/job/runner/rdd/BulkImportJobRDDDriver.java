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

import sleeper.bulkimport.job.runner.BulkImportJobDriver;
import sleeper.bulkimport.job.runner.BulkImportJobInput;
import sleeper.bulkimport.job.runner.BulkImportJobRunner;
import sleeper.bulkimport.job.runner.SparkFileInfoRow;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;

import java.io.IOException;

/**
 * This class runs {@link BulkImportJobDriver} with a {@link BulkImportJobRunner} which
 * uses the Spark RDD API to partition the data according to the Sleeper
 * partitions and for each partition, write a single sorted Parquet file.
 */
public class BulkImportJobRDDDriver {
    private BulkImportJobRDDDriver() {
    }

    public static void main(String[] args) throws Exception {
        BulkImportJobDriver.start(args, BulkImportJobRDDDriver::createFileInfos);
    }

    public static Dataset<Row> createFileInfos(BulkImportJobInput input) throws IOException {
        Schema schema = input.tableProperties().getSchema();
        String schemaAsString = new SchemaSerDe().toJson(schema);
        JavaRDD<Row> rdd = input.rows().javaRDD()
                .mapToPair(new ExtractKeyFunction(
                        schema.getRowKeyTypes().size() + schema.getSortKeyTypes().size())) // Sort by both row keys and sort keys
                .repartitionAndSortWithinPartitions(
                        new SleeperPartitioner(schemaAsString, input.broadcastedPartitions()),
                        new WrappedKeyComparator(schemaAsString))
                .map(tuple -> tuple._2)
                .mapPartitions(new WriteParquetFile(
                        input.instanceProperties().saveAsString(),
                        input.tableProperties().saveAsString(),
                        input.conf(), input.broadcastedPartitions()));

        SparkSession session = SparkSession.builder().getOrCreate();
        return session.createDataset(rdd.rdd(), RowEncoder.apply(SparkFileInfoRow.createFileInfoSchema()));
    }
}
