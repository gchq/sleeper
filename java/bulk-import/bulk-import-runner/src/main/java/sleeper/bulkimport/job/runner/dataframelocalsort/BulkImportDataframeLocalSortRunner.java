/*
 * Copyright 2022 Crown Copyright
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
package sleeper.bulkimport.job.runner.dataframelocalsort;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.bulkimport.job.BulkImportJob;
import sleeper.bulkimport.job.runner.BulkImportJobRunner;
import sleeper.bulkimport.job.runner.rdd.WriteParquetFile;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.schema.Schema;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The {@link BulkImportDataframeLocalSortRunner} is a {@link BulkImportJobRunner} which
 * uses Spark's Dataframe API to efficiently sort and write out the data split by
 * Sleeoer partition.
 */
public class BulkImportDataframeLocalSortRunner extends BulkImportJobRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkImportDataframeLocalSortRunner.class);

    @Override
    public Dataset<Row> createFileInfos(
            Dataset<Row> rows,
            BulkImportJob job,
            TableProperties tableProperties,
            Broadcast<List<Partition>> broadcastedPartitions, Configuration conf) throws IOException {
        LOGGER.info("Running bulk import job with id {}", job.getId());

        Schema schema = tableProperties.getSchema();
        int numLeafPartitions = (int) broadcastedPartitions.value().stream().filter(Partition::isLeafPartition).count();
        LOGGER.info("There are {} leaf partitions", numLeafPartitions);

        PartitionTree partitionTree = new PartitionTree(schema, broadcastedPartitions.getValue());
        Dataset<Row> dataWithPartition = rows.withColumn(PARTITION_FIELD_NAME, PartitionAsIntColumn.getColumn(partitionTree, schema));

        LOGGER.info("After adding partition id as int, there are {} partitions", dataWithPartition.rdd().getNumPartitions());

        Dataset<Row> repartitionedData = new com.joom.spark.package$implicits$ExplicitRepartitionWrapper(dataWithPartition).explicitRepartition(numLeafPartitions, new Column(PARTITION_FIELD_NAME));
        LOGGER.info("After repartitioning data, there are {} partitions", repartitionedData.rdd().getNumPartitions());

        Column[] sortColumns = Lists.newArrayList(schema.getRowKeyFieldNames(), schema.getSortKeyFieldNames())
                .stream()
                .flatMap(list -> ((List<String>) list).stream())
                .map(Column::new)
                .collect(Collectors.toList())
                .toArray(new Column[0]);
        LOGGER.info("Sorting by columns {}", String.join(",", Arrays.stream(sortColumns).map(c -> c.toString()).collect(Collectors.toList())));

        Dataset<Row> sortedRows = repartitionedData.sortWithinPartitions(sortColumns);
        LOGGER.info("There are {} partitions in the sorted-within-partition Dataset", sortedRows.rdd().getNumPartitions());

        return sortedRows.mapPartitions(new WriteParquetFile(getInstanceProperties().saveAsString(), tableProperties.saveAsString(), conf, broadcastedPartitions), RowEncoder.apply(createFileInfoSchema()));
    }

    public static void main(String[] args) throws Exception {
        BulkImportJobRunner.start(args, new BulkImportDataframeLocalSortRunner());
    }
}
