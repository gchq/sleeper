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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;

import sleeper.bulkimport.runner.BulkImportJobDriver;
import sleeper.bulkimport.runner.BulkImportSparkContext;
import sleeper.bulkimport.runner.common.SparkFileReferenceRow;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;

/**
 * Runs a bulk import job using Spark's RDD API. Sorts and writes out the data split by Sleeper partition.
 */
public class BulkImportJobRDDDriver {
    private BulkImportJobRDDDriver() {
    }

    public static void main(String[] args) throws Exception {
        BulkImportJobDriver.start(args, BulkImportJobRDDDriver::createFileReferences);
    }

    public static Dataset<Row> createFileReferences(BulkImportSparkContext input) {
        Schema schema = input.getTableProperties().getSchema();
        String schemaAsString = new SchemaSerDe().toJson(schema);
        JavaRDD<Row> rdd = input.getRows().javaRDD()
                .mapToPair(new ExtractKeyFunction(
                        schema.getRowKeyTypes().size() + schema.getSortKeyTypes().size())) // Sort by both row keys and sort keys
                .repartitionAndSortWithinPartitions(
                        new SleeperPartitioner(schemaAsString, input.getPartitionsBroadcast()),
                        new WrappedKeyComparator(schemaAsString))
                .map(tuple -> tuple._2)
                .mapPartitions(new WriteParquetFile(
                        input.getInstanceProperties().saveAsString(),
                        input.getTableProperties().saveAsString(),
                        input.getHadoopConf(), input.getPartitionsBroadcast()));

        SparkSession session = SparkSession.builder().getOrCreate();
        return session.createDataset(rdd.rdd(), ExpressionEncoder.apply(SparkFileReferenceRow.createFileReferenceSchema()));
    }
}
