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
package sleeper.bulkimport.runner.dataframelocalsort;

import com.google.common.collect.Lists;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.runner.BulkImportJobDriver;
import sleeper.bulkimport.runner.BulkImportSparkContext;
import sleeper.bulkimport.runner.common.SparkFileReferenceRow;
import sleeper.bulkimport.runner.rdd.WriteParquetFile;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Runs a bulk import job using Spark's Dataframe API, sorting locally in each partition. Sorts and writes out the data
 * split by Sleeper partition.
 */
public class BulkImportDataframeLocalSortDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkImportDataframeLocalSortDriver.class);

    private BulkImportDataframeLocalSortDriver() {
    }

    public static void main(String[] args) throws Exception {
        BulkImportJobDriver.start(args, BulkImportDataframeLocalSortDriver::createFileReferences);
    }

    public static Dataset<Row> createFileReferences(BulkImportSparkContext input) {
        Dataset<Row> repartitionedData = RepartitionRowsBySleeperPartition.repartition(input);
        LOGGER.info("After repartitioning data, there are {} partitions", repartitionedData.rdd().getNumPartitions());

        Column[] sortColumns = Lists.newArrayList(input.getSchema().getRowKeyFieldNames(), input.getSchema().getSortKeyFieldNames())
                .stream()
                .flatMap(Collection::stream)
                .map(Column::new)
                .toArray(Column[]::new);
        LOGGER.info("Sorting by columns {}", Arrays.stream(sortColumns)
                .map(Column::toString)
                .collect(Collectors.joining(",")));

        Dataset<Row> sortedRows = repartitionedData.sortWithinPartitions(sortColumns);
        LOGGER.info("There are {} partitions in the sorted-within-partition Dataset", sortedRows.rdd().getNumPartitions());

        return sortedRows.mapPartitions(
                new WriteParquetFile(
                        input.getInstanceProperties().saveAsString(),
                        input.getTableProperties().saveAsString(),
                        input.getHadoopConf(), input.getPartitionsBroadcast()),
                ExpressionEncoder.apply(SparkFileReferenceRow.createFileReferenceSchema()));
    }
}
