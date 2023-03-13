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

package sleeper.bulkimport.job.runner.dataframelocalsort;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.spark.serializer.KryoRegistrator;

import java.util.List;

public final class JdkImmutableListRegistrator implements KryoRegistrator {
    @Override
    public void registerClasses(final Kryo kryo) {
        final ImmutableListSerializer serializer = new ImmutableListSerializer();
        kryo.register(List.of().getClass(), serializer);
        kryo.register(List.of(1).getClass(), serializer);
        kryo.register(List.of(1, 2, 3, 4).getClass(), serializer);
        kryo.register(List.of(1, 2, 3, 4).subList(0, 2).getClass(), serializer);
        kryo.register(List.of(1, 2, 3, 4).iterator().getClass(), serializer);
    }

    public static class ImmutableListSerializer extends Serializer<List<Object>> {
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
