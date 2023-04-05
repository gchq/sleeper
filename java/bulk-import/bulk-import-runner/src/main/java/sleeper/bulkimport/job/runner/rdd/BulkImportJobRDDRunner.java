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
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.bulkimport.job.runner.BulkImportJobRunner;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;

import java.io.IOException;
import java.util.List;

/**
 * The {@link BulkImportJobRDDRunner} is a {@link BulkImportJobRunner} which
 * uses the Spark RDD API to partition the data according to the Sleeper
 * partitions and for each partition, write a single sorted Parquet file.
 */
public class BulkImportJobRDDRunner extends BulkImportJobRunner {

    @Override
    public Dataset<Row> createFileInfos(
            Dataset<Row> rows, BulkImportJob job, TableProperties tableProperties,
            Broadcast<List<Partition>> broadcastedPartitions, Configuration conf) throws IOException {
        BulkImportRDDPartitioner partitioner = new BulkImportRDDPartitioner();
        return partitioner.createFileInfos(rows, job, getInstanceProperties(), tableProperties, broadcastedPartitions, conf);
    }

    public static void main(String[] args) throws Exception {
        BulkImportJobRunner.start(args, new BulkImportJobRDDRunner());
    }
}
