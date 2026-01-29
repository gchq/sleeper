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

import org.apache.datasketches.quantiles.ItemsUnion;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.runner.BulkImportSparkContext;
import sleeper.bulkimport.runner.common.SparkSketchBytesRow;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.sketches.Sketches;
import sleeper.sketches.SketchesSerDe;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

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
        Dataset<Row> sketchFiles = input.getRows().mapPartitions(
                new GenerateSketches(
                        input.getInstanceProperties(), input.getTableProperties(),
                        input.getPartitionsBroadcast()),
                ExpressionEncoder.apply(SparkSketchBytesRow.createSchema()));
        Schema schema = input.getTableProperties().getSchema();
        SketchesSerDe serDe = new SketchesSerDe(schema);
        Map<String, SketchesBuilder> partitionIdToBuilder = new HashMap<>();
        sketchFiles.collectAsList().stream()
                .map(SparkSketchBytesRow::from)
                .forEach(row -> {
                    Sketches sketches = serDe.fromBytes(row.sketchBytes());
                    SketchesBuilder builder = partitionIdToBuilder.computeIfAbsent(
                            row.partitionId(), id -> new SketchesBuilder(schema));
                    builder.add(sketches);
                });
        return partitionIdToBuilder.entrySet().stream()
                .collect(toMap(Entry::getKey, entry -> entry.getValue().build()));
    }

    private static class SketchesBuilder {
        private final Schema schema;
        private final Map<String, ItemsUnion<Object>> fieldNameToUnion;

        SketchesBuilder(Schema schema) {
            this.schema = schema;
            this.fieldNameToUnion = schema.getRowKeyFields().stream()
                    .collect(toMap(
                            Field::getName,
                            field -> Sketches.createUnion(field.getType())));
        }

        void add(Sketches sketches) {
            for (Field field : schema.getRowKeyFields()) {
                ItemsUnion<Object> union = fieldNameToUnion.get(field.getName());
                union.update(sketches.getQuantilesSketch(field.getName()));
            }
        }

        Sketches build() {
            return new Sketches(schema, fieldNameToUnion.entrySet().stream()
                    .collect(toMap(Entry::getKey, entry -> entry.getValue().getResult())));
        }
    }
}
