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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.runner.BulkImportSparkContext;
import sleeper.bulkimport.runner.common.HadoopSketchesStore;
import sleeper.bulkimport.runner.common.SparkSketchRow;
import sleeper.bulkimport.runner.dataframelocalsort.RepartitionRowsBySleeperPartition;
import sleeper.sketches.Sketches;
import sleeper.sketches.store.SketchesStore;

import java.util.Map;

import static java.util.stream.Collectors.toMap;

/**
 * Generates sketches by reading through data in Spark. Used to calculate split points when pre-splitting partitions.
 */
public class GenerateSketchesDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(GenerateSketchesDriver.class);

    private GenerateSketchesDriver() {
    }

    /**
     * Reads through all the input data in a bulk import job, and builds a sketch of the data in each Sleeper partition.
     *
     * @param  input the context of the bulk import
     * @return       a map from Sleeper partition ID to a sketch of the data in that partition
     */
    public static Map<String, Sketches> generatePartitionIdToSketches(BulkImportSparkContext input) {
        LOGGER.info("Generating sketches...");
        Dataset<Row> partitioned = RepartitionRowsBySleeperPartition.repartition(input);
        Dataset<Row> sketchFiles = partitioned.mapPartitions(
                new WriteSketchesFile(
                        input.getInstanceProperties(), input.getTableProperties(),
                        input.getHadoopConf(), input.getPartitionsBroadcast()),
                ExpressionEncoder.apply(SparkSketchRow.createSchema()));
        SketchesStore sketchesStore = new HadoopSketchesStore(input.getHadoopConf());
        return sketchFiles.collectAsList().stream()
                .map(SparkSketchRow::from)
                .collect(toMap(
                        SparkSketchRow::partitionId,
                        row -> sketchesStore.loadFileSketches(row.filename(), input.getSchema())));
    }

}
