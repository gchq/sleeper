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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import sleeper.core.statestore.AllReferencesToAFile;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public class StateStoreArrowFiles {

    private static final Field FILENAME = Field.notNullable("filename", Utf8.INSTANCE);
    private static final Field UPDATE_TIME = Field.notNullable("updateTime", Types.MinorType.TIMESTAMPMILLI.getType());
    private static final Schema FILES_SCHEMA = new Schema(List.of(FILENAME, UPDATE_TIME));

    private StateStoreArrowFiles() {
    }

    public static void write(Collection<AllReferencesToAFile> files, BufferAllocator allocator, WritableByteChannel channel) throws IOException {
        try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(FILES_SCHEMA, allocator);
                ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, null, channel)) {
            vectorSchemaRoot.getFieldVectors().forEach(fieldVector -> fieldVector.setInitialCapacity(files.size()));
            vectorSchemaRoot.allocateNew();
            writer.start();
            for (AllReferencesToAFile file : files) {
                vectorSchemaRoot.getVector(FILENAME);
            }

        }
    }

    public static Stream<AllReferencesToAFile> read(ReadableByteChannel channel) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'read'");
    }
}
