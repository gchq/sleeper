/*
 * Copyright 2022-2024 Crown Copyright
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

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;

import java.util.Iterator;
import java.util.List;

/**
 * Adds an integer ID to each row identifying which Sleeper partition it belongs to. Uses
 * {@link AddPartitionAsIntIterator}.
 */
public class AddPartitionAsIntFunction implements MapPartitionsFunction<Row, Row> {
    private static final long serialVersionUID = 4871009858051824361L;

    private final String schemaAsString;
    private final Broadcast<List<Partition>> broadcastPartitions;

    public AddPartitionAsIntFunction(String schemaAsString, Broadcast<List<Partition>> broadcastPartitions) {
        this.schemaAsString = schemaAsString;
        this.broadcastPartitions = broadcastPartitions;
    }

    @Override
    public Iterator<Row> call(Iterator<Row> input) {
        Schema schema = new SchemaSerDe().fromJson(schemaAsString);
        List<Partition> partitions = broadcastPartitions.getValue();
        PartitionTree partitionTree = new PartitionTree(partitions);
        return new AddPartitionAsIntIterator(input, schema, partitionTree);
    }
}
