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

package sleeper.bulkimport.runner.common;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.spark.serializer.KryoRegistrator;

import java.util.List;

public final class JdkImmutableListRegistrator implements KryoRegistrator {
    @Override
    public void registerClasses(Kryo kryo) {
        ImmutableListSerializer serializer = new ImmutableListSerializer();

        // The field Partition.rowKeyTypes is populated from Schema.getRowKeyTypes,
        // which uses Collectors.toUnmodifiableList. In the JDK we've tested, this will produce
        // either a ListN or a List12, depending on the number of row keys.

        // ImmutableCollections.ListN is package-private
        kryo.register(List.of().getClass(), serializer);

        // ImmutableCollections.List12 is package-private
        kryo.register(List.of(1).getClass(), serializer);
    }

    private static class ImmutableListSerializer extends Serializer<List<Object>> {
        @Override
        public void write(Kryo kryo, Output output, List<Object> object) {
            output.writeInt(object.size(), true);
            for (final Object elm : object) {
                kryo.writeClassAndObject(output, elm);
            }
        }

        @Override
        public List<Object> read(Kryo kryo, Input input, Class<List<Object>> type) {
            final int size = input.readInt(true);
            final Object[] list = new Object[size];
            for (int i = 0; i < size; ++i) {
                list[i] = kryo.readClassAndObject(input);
            }
            return List.of(list);
        }
    }
}
