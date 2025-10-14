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
package sleeper.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.util.Text;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import sleeper.core.row.Row;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIndexOutOfBoundsException;

class ArrowToRowConversionUtilsTest {

    private BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    @Test
    void shouldConvertPrimitiveIntType() {
        // Given
        try (
                IntVector intVec = new IntVector("intField", allocator);
                VarCharVector strVec = new VarCharVector("strField", allocator)) {
            intVec.allocateNew(1);
            intVec.setSafe(0, 123);
            intVec.setValueCount(1);

            strVec.allocateNew(10, 1);
            strVec.setSafe(0, new Text("hello"));
            strVec.setValueCount(1);

            VectorSchemaRoot root = new VectorSchemaRoot(Arrays.asList(intVec, strVec));

            // When
            Row row = ArrowToRowConversionUtils.convertVectorSchemaRootToRow(root, 0);

            // Then
            assertThat(row.get("intField")).isEqualTo(123);
            assertThat(row.get("strField")).isEqualTo("hello");
        }
    }

    @Test
    void shouldConvertPrimitiveLongType() {
        // Given
        try (
                BigIntVector longVec = new BigIntVector("longField", allocator);
                VarCharVector strVec = new VarCharVector("strField", allocator)) {
            longVec.allocateNew(1);
            longVec.setSafe(0, 123);
            longVec.setValueCount(1);

            strVec.allocateNew(10, 1);
            strVec.setSafe(0, new Text("hello"));
            strVec.setValueCount(1);

            VectorSchemaRoot root = new VectorSchemaRoot(Arrays.asList(longVec, strVec));

            // When
            Row row = ArrowToRowConversionUtils.convertVectorSchemaRootToRow(root, 0);

            // Then
            assertThat(row.get("longField")).isEqualTo(123L);
            assertThat(row.get("strField")).isEqualTo("hello");
        }
    }

    @Test
    void shouldConvertPrimitiveByteTypes() {
        // Given
        try (
                VarBinaryVector byteVec = new VarBinaryVector("byteField", allocator);
                VarCharVector strVec = new VarCharVector("strField", allocator)) {
            byteVec.allocateNew(1);
            byteVec.setSafe(0, new byte[]{0, 1, 2, 3, 4});
            byteVec.setValueCount(1);

            strVec.allocateNew(10, 1);
            strVec.setSafe(0, new Text("hello"));
            strVec.setValueCount(1);

            VectorSchemaRoot root = new VectorSchemaRoot(Arrays.asList(byteVec, strVec));

            // When
            Row row = ArrowToRowConversionUtils.convertVectorSchemaRootToRow(root, 0);

            // Then
            assertThat(row.get("byteField")).isEqualTo(new byte[]{0, 1, 2, 3, 4});
            assertThat(row.get("strField")).isEqualTo("hello");
        }
    }

    @Test
    void shouldConvertListType() {
        // Given
        try (
                ListVector listVec = ListVector.empty("listField", allocator)) {
            listVec.allocateNew();
            UnionListWriter values = listVec.getWriter();
            values.setPosition(0);
            values.startList();
            values.writeInt(1);
            values.writeInt(2);
            values.writeInt(3);
            values.setValueCount(3);
            values.endList();
            listVec.setValueCount(1);

            VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(listVec));

            // When
            Row row = ArrowToRowConversionUtils.convertVectorSchemaRootToRow(root, 0);

            // Then
            assertThat(row.get("listField"))
                    .asInstanceOf(InstanceOfAssertFactories.LIST)
                    .containsExactly(1, 2, 3);
        }
    }

    @Test
    void shouldConvertMapType() {
        // Given
        try (
                MapVector mapVector = MapVector.empty("mapField", allocator, false)) {
            mapVector.allocateNew();
            mapVector.setValueCount(1);
            UnionMapWriter mapWriter = mapVector.getWriter();
            mapWriter.setPosition(0);
            mapWriter.setValueCount(2);
            mapWriter.startMap();
            mapWriter.startEntry();
            mapWriter.key().integer().writeInt(1);
            mapWriter.value().integer().writeInt(2);
            mapWriter.endEntry();
            mapWriter.startEntry();
            mapWriter.key().integer().writeInt(3);
            mapWriter.value().integer().writeInt(4);
            mapWriter.endEntry();
            mapWriter.endMap();

            VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(mapVector));

            Map<Integer, Integer> expected = Map.of(1, 2, 3, 4);

            // When
            Row row = ArrowToRowConversionUtils.convertVectorSchemaRootToRow(root, 0);

            // Then
            assertThat(row.get("mapField")).asInstanceOf(InstanceOfAssertFactories.MAP)
                    .containsExactlyInAnyOrderEntriesOf(expected);
        }
    }

    @Test
    void shouldConvertNestedListInMap() {
        // Given
        try (
                MapVector mapVector = MapVector.empty("mapField", allocator, false)) {
            mapVector.allocateNew();
            ListWriter valueWriter;
            UnionMapWriter mapWriter = mapVector.getWriter();
            mapWriter.setPosition(0);
            mapWriter.startMap();
            mapWriter.startEntry();
            mapWriter.key().integer().writeInt(1);
            valueWriter = mapWriter.value().list();
            valueWriter.startList();
            valueWriter.integer().writeInt(2);
            valueWriter.integer().writeInt(3);
            valueWriter.endList();
            mapWriter.endEntry();

            mapWriter.startEntry();
            mapWriter.key().integer().writeInt(4);
            valueWriter = mapWriter.value().list();
            valueWriter.startList();
            valueWriter.integer().writeInt(5);
            valueWriter.integer().writeInt(6);
            valueWriter.endList();
            mapWriter.endEntry();
            mapWriter.endMap();
            mapWriter.setValueCount(1);

            VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(mapVector));

            Map<Integer, List<Integer>> expected = Map.of(1, List.of(2, 3), 4, List.of(5, 6));

            // When
            Row row = ArrowToRowConversionUtils.convertVectorSchemaRootToRow(root, 0);

            // Then
            assertThat(row.get("mapField")).asInstanceOf(InstanceOfAssertFactories.MAP)
                    .containsExactlyInAnyOrderEntriesOf(expected);
        }
    }

    @Test
    void shouldConvertNestedListInList() {
        // Given
        try (
                ListVector listVector = ListVector.empty("listField", allocator)) {
            listVector.allocateNew();
            UnionListWriter listWriter = listVector.getWriter();
            listWriter.setPosition(0);
            listWriter.startList();
            listWriter.list().startList();
            listWriter.list().integer().writeInt(1);
            listWriter.list().integer().writeInt(2);
            listWriter.list().integer().writeInt(3);
            listWriter.list().endList();
            listWriter.list().startList();
            listWriter.list().integer().writeInt(4);
            listWriter.list().integer().writeInt(5);
            listWriter.list().integer().writeInt(6);
            listWriter.list().endList();
            listWriter.endList();
            listWriter.setValueCount(1);

            VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(listVector));

            List<List<Integer>> expected = List.of(List.of(1, 2, 3), List.of(4, 5, 6));

            // When
            Row row = ArrowToRowConversionUtils.convertVectorSchemaRootToRow(root, 0);

            // Then
            assertThat(row.get("listField")).asInstanceOf(InstanceOfAssertFactories.LIST).containsExactly(expected.toArray());
        }
    }

    @Test
    void shouldConvertNestedMapInMap() {
        // Given
        try (
                MapVector mapVector = MapVector.empty("mapField", allocator, false)) {
            mapVector.allocateNew();
            MapWriter valueWriter;
            UnionMapWriter mapWriter = mapVector.getWriter();
            mapWriter.setPosition(0);
            mapWriter.startMap();
            mapWriter.startEntry();
            mapWriter.key().integer().writeInt(1);
            valueWriter = mapWriter.value().map(false);
            valueWriter.startMap();
            valueWriter.startEntry();
            valueWriter.key().integer().writeInt(2);
            valueWriter.value().integer().writeInt(3);
            valueWriter.endEntry();
            valueWriter.startEntry();
            valueWriter.key().integer().writeInt(4);
            valueWriter.value().integer().writeInt(5);
            valueWriter.endEntry();
            valueWriter.endMap();
            mapWriter.endEntry();
            mapWriter.startEntry();
            mapWriter.key().integer().writeInt(6);
            valueWriter = mapWriter.value().map(false);
            valueWriter.startMap();
            valueWriter.startEntry();
            valueWriter.key().integer().writeInt(7);
            valueWriter.value().integer().writeInt(8);
            valueWriter.endEntry();
            valueWriter.startEntry();
            valueWriter.key().integer().writeInt(9);
            valueWriter.value().integer().writeInt(10);
            valueWriter.endEntry();
            valueWriter.endMap();
            mapWriter.endEntry();

            mapWriter.endMap();
            mapWriter.setValueCount(1);

            VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(mapVector));

            Map<Integer, Map<Integer, Integer>> expected = Map.of(1, Map.of(2, 3, 4, 5), 6, Map.of(7, 8, 9, 10));

            // When
            Row row = ArrowToRowConversionUtils.convertVectorSchemaRootToRow(root, 0);

            // Then
            assertThat(row.get("mapField")).asInstanceOf(InstanceOfAssertFactories.MAP)
                    .containsExactlyInAnyOrderEntriesOf(expected);
        }
    }

    @Test
    void shouldConvertNestedMapInList() {
        // Given
        try (
                ListVector listVector = ListVector.empty("listField", allocator)) {
            listVector.allocateNew();
            MapWriter valueWriter;
            UnionListWriter listWriter = listVector.getWriter();
            listWriter.setPosition(0);
            listWriter.startList();
            valueWriter = listWriter.map(false);
            valueWriter.startMap();
            valueWriter.startEntry();
            valueWriter.key().integer().writeInt(1);
            valueWriter.value().integer().writeInt(2);
            valueWriter.endEntry();
            valueWriter.startEntry();
            valueWriter.key().integer().writeInt(3);
            valueWriter.value().integer().writeInt(4);
            valueWriter.endEntry();
            valueWriter.endMap();
            valueWriter = listWriter.map(false);
            valueWriter.startMap();
            valueWriter.startEntry();
            valueWriter.key().integer().writeInt(5);
            valueWriter.value().integer().writeInt(6);
            valueWriter.endEntry();
            valueWriter.startEntry();
            valueWriter.key().integer().writeInt(7);
            valueWriter.value().integer().writeInt(8);
            valueWriter.endEntry();
            valueWriter.endMap();

            listWriter.endList();
            listWriter.setValueCount(1);

            VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(listVector));

            List<Map<Integer, Integer>> expected = List.of(Map.of(1, 2, 3, 4), Map.of(5, 6, 7, 8));

            // When
            Row row = ArrowToRowConversionUtils.convertVectorSchemaRootToRow(root, 0);

            // Then
            assertThat(row.get("listField")).asInstanceOf(InstanceOfAssertFactories.LIST).containsExactly(expected.toArray());
        }
    }

    @Test
    void shouldThrowExceptionOnOutOfBounds() {
        // Given
        try (
                IntVector intVec = new IntVector("intField", allocator)) {
            intVec.allocateNew(1);
            intVec.setSafe(0, 123);
            intVec.setValueCount(1);

            VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(intVec));

            // Expect
            assertThatIndexOutOfBoundsException().isThrownBy(() -> ArrowToRowConversionUtils.convertVectorSchemaRootToRow(root, 1));
        }
    }
}
