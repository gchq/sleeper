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
package sleeper.bulkimport.runner.dataframe;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class AddPartitionIteratorTest {

    @Test
    public void shouldAddPartitionField() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .sortKeyFields(new Field("sort", new LongType()))
                .valueFields(new Field("value", new StringType()))
                .build();
        Iterator<Row> rows = List.of(
                RowFactory.create(1, 2L, "3"),
                RowFactory.create(4, 5L, "6")).iterator();
        PartitionTree partitionTree = new PartitionsBuilder(schema).rootFirst("root")
                .splitToNewChildren("root", "L", "R", 3)
                .buildTree();
        AddPartitionIterator addPartitionIterator = new AddPartitionIterator(rows, schema, partitionTree);

        // When / Then
        assertThat(addPartitionIterator).toIterable()
                .containsExactly(
                        RowFactory.create(1, 2L, "3", "L"),
                        RowFactory.create(4, 5L, "6", "R"));
    }
}
