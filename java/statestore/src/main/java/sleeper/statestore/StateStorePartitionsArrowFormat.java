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
package sleeper.statestore;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import sleeper.core.partition.Partition;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.List;

/**
 * Reads and writes the state of partitions in a state store to an Arrow file.
 */
public class StateStorePartitionsArrowFormat {
    private static final Field ID = Field.notNullable("id", Utf8.INSTANCE);
    private static final Field PARENT_ID = Field.nullable("parentId", Utf8.INSTANCE);
    private static final Field CHILD_ID = Field.notNullable("childId", Utf8.INSTANCE);
    private static final Field CHILD_IDS = new Field("childIds",
            FieldType.notNullable(Types.MinorType.LIST.getType()), List.of(CHILD_ID));
    private static final Field IS_LEAF = Field.notNullable("isLeaf", Types.MinorType.BIT.getType());
    private static final Field DIMENSION = Field.nullable("dimension", Types.MinorType.UINT4.getType());

    private static final Field FIELD = Field.notNullable("field", Utf8.INSTANCE);
    private static final Field MIN = Field.notNullable("min", Utf8.INSTANCE);
    private static final Field MAX = Field.notNullable("max", Utf8.INSTANCE);
    private static final Field RANGE = new Field("range",
            FieldType.notNullable(Types.MinorType.STRUCT.getType()), List.of(FIELD, MIN, MAX));
    private static final Field REGION = new Field("region",
            FieldType.notNullable(Types.MinorType.LIST.getType()), List.of(RANGE));

    private StateStorePartitionsArrowFormat() {
    }

    /**
     * Writes the state of partitions in Arrow format.
     *
     * @param  partitions  the partitions in the state store
     * @param  allocator   the buffer allocator
     * @param  channel     the channel to write to
     * @throws IOException if writing to the channel fails
     */
    public static void write(Collection<Partition> partitions, BufferAllocator allocator, WritableByteChannel channel) throws IOException {
    }
}
