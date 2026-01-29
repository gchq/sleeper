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

import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.runner.common.SparkRowMapper;
import sleeper.bulkimport.runner.common.SparkSketchBytesRow;
import sleeper.bulkimport.runner.common.SparkSketchRow;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.sketches.Sketches;
import sleeper.sketches.SketchesSerDe;

import java.util.Iterator;

/**
 * An iterator that writes a single data sketch for all the input data. This takes any number of rows, adds them to a
 * sketch, writes that sketch to a file, then returns a single Spark row that references that file. The resulting row
 * can be read with {@link SparkSketchRow}.
 */
public class SketchByteWritingterator implements Iterator<Row> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SketchByteWritingterator.class);

    private final Iterator<Row> input;
    private final Schema schema;
    private final SparkRowMapper rowMapper;
    private final PartitionTree partitionTree;

    public SketchByteWritingterator(Iterator<Row> input, TableProperties tableProperties, PartitionTree partitionTree) {
        this.input = input;
        this.schema = tableProperties.getSchema();
        this.rowMapper = new SparkRowMapper(tableProperties.getSchema());
        this.partitionTree = partitionTree;
        LOGGER.info("Initialised SketchWritingAsFileIterator");
    }

    @Override
    public boolean hasNext() {
        return input.hasNext();
    }

    @Override
    public Row next() {
        Sketches sketches = Sketches.from(schema);
        SketchesSerDe serDe = new SketchesSerDe(schema);
        Partition partition = null;
        int numRows = 0;
        while (input.hasNext()) {
            Row row = input.next();
            sleeper.core.row.Row sleeperRow = rowMapper.toSleeperRow(row);
            if (partition == null) {
                partition = partitionTree.getLeafPartition(schema, sleeperRow.getRowKeys(schema));
                LOGGER.info("Found data for partition {}", partition.getId());
            }
            sketches.update(sleeperRow);
            numRows++;
            if (numRows % 1_000_000L == 0) {
                LOGGER.info("Read {} rows", numRows);
            }
        }
        LOGGER.info("Writing sketches file for partition {}", partition.getId());
        return new SparkSketchBytesRow(partition.getId(), serDe.toBytes(sketches)).toSparkRow();
    }

}
