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

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.runner.common.HadoopSketchesStore;
import sleeper.bulkimport.runner.common.PartitionNumbers;
import sleeper.bulkimport.runner.common.SparkRowMapper;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.sketches.Sketches;
import sleeper.sketches.store.SketchesStore;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;

public class SketchWritingIterator implements Iterator<Row> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SketchWritingIterator.class);

    private final Iterator<Row> input;
    private final InstanceProperties instanceProperties;
    private final Schema schema;
    private final SparkRowMapper rowMapper;
    private final List<Partition> intToPartition;
    private final SketchesStore sketchesStore;

    public SketchWritingIterator(Iterator<Row> input, InstanceProperties instanceProperties, TableProperties tableProperties, Configuration conf, PartitionTree partitionTree) {
        this.input = input;
        this.instanceProperties = instanceProperties;
        this.schema = tableProperties.getSchema();
        this.rowMapper = new SparkRowMapper(tableProperties.getSchema());
        this.intToPartition = PartitionNumbers.getIntToPartition(partitionTree);
        this.sketchesStore = new HadoopSketchesStore(conf);
        LOGGER.info("Initialised SketchWritingIterator");
    }

    @Override
    public boolean hasNext() {
        return input.hasNext();
    }

    @Override
    public Row next() {
        Sketches sketches = Sketches.from(schema);
        int partitionInt = -1;
        int numRows = 0;
        while (input.hasNext()) {
            Row row = input.next();
            int thisPartitionInt = rowMapper.getPartitionInt(row);
            if (partitionInt == -1) {
                partitionInt = thisPartitionInt;
                LOGGER.info("Found data for partition {}", intToPartition.get(partitionInt));
            } else if (partitionInt != thisPartitionInt) {
                throw new RuntimeException("Row found on different partition, expected " + partitionInt + ", found " + thisPartitionInt);
            }
            sketches.update(rowMapper.toSleeperRow(row));
            numRows++;
            if (numRows % 1_000_000L == 0) {
                LOGGER.info("Read {} rows", numRows);
            }
        }
        Partition partition = intToPartition.get(partitionInt);
        String filename = instanceProperties.get(FILE_SYSTEM) + instanceProperties.get(BULK_IMPORT_BUCKET) + "/sketches/" + UUID.randomUUID().toString() + ".sketches";
        LOGGER.info("Writing sketches file for partition {}", partition.getId());
        sketchesStore.saveFileSketches(filename, schema, sketches);
        return RowFactory.create(partition.getId(), filename);
    }

}
