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
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.junit.jupiter.api.Test;

import sleeper.bulkimport.runner.BulkImportSparkContext;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionFactory;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class KryoSerializerTest {

    @Test
    void shouldSerializeAndDeserializePartitionWithOneRowKey() {
        // Given
        Kryo kryo = kryo();
        Partition partition = partitionWithNRowKeys(1);

        // When
        byte[] bytes = serialize(kryo, partition);

        // Then
        assertThat(deserialize(kryo, bytes, Partition.class)).isEqualTo(partition);
    }

    @Test
    void shouldSerializeAndDeserializePartitionWithTwoRowKeys() {
        // Given
        Kryo kryo = kryo();
        Partition partition = partitionWithNRowKeys(2);

        // When
        byte[] bytes = serialize(kryo, partition);

        // Then
        assertThat(deserialize(kryo, bytes, Partition.class)).isEqualTo(partition);
    }

    @Test
    void shouldSerializeAndDeserializePartitionWithManyRowKeys() {
        // Given
        Kryo kryo = kryo();
        Partition partition = partitionWithNRowKeys(10);

        // When
        byte[] bytes = serialize(kryo, partition);

        // Then
        assertThat(deserialize(kryo, bytes, Partition.class)).isEqualTo(partition);
    }

    @Test
    void shouldFailToDeserializeClassWithImmutableListIfImmutableListsNotRegistered() {
        // Given
        Kryo kryo = kryoWithoutImmutableListSupport();
        ImmutableListWrapper immutableListWrapper = new ImmutableListWrapper();

        // When / Then
        byte[] bytes = serialize(kryo, immutableListWrapper);
        assertThatThrownBy(() -> deserialize(kryo, bytes, ImmutableListWrapper.class))
                .isInstanceOf(KryoException.class)
                .hasCauseInstanceOf(UnsupportedOperationException.class);
    }

    private static Partition partitionWithNRowKeys(int n) {
        return new PartitionFactory(Schema.builder()
                .rowKeyFields(IntStream.rangeClosed(1, n)
                        .mapToObj(i -> new Field("key-" + i, new LongType()))
                        .collect(Collectors.toUnmodifiableList()))
                .build())
                .rootFirst("test-partition");
    }

    private static Kryo kryo() {
        SparkConf sparkConf = BulkImportSparkContext.createSparkConf();
        return new KryoSerializer(sparkConf).newKryo();
    }

    private static Kryo kryoWithoutImmutableListSupport() {
        SparkConf sparkConf = BulkImportSparkContext.createSparkConf();
        sparkConf.remove("spark.kryo.registrator");
        return new KryoSerializer(sparkConf).newKryo();
    }

    private static byte[] serialize(Kryo kryo, Object object) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (Output output = new Output(outputStream)) {
            kryo.writeObject(output, object);
        }
        return outputStream.toByteArray();
    }

    private static <T> T deserialize(Kryo kryo, byte[] bytes, Class<T> readClass) {
        try (Input input = new Input(new ByteArrayInputStream(bytes))) {
            return kryo.readObject(input, readClass);
        }
    }

    private static class ImmutableListWrapper {
        List<String> immutableList = List.of("test");
    }
}
