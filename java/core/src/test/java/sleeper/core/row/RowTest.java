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
package sleeper.core.row;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RowTest {

    @Test
    public void testAnFieldCanBeRemoved() {
        //Given
        Row row = new Row();
        row.put("key", "value");

        //when
        row.remove("key");

        //then
        assertThat(row.get("key")).isNull();
    }

    @Test
    public void testAnNonExistentFieldCanBeRemoved() {
        //Given
        Row row = new Row();

        //when
        row.remove("key");

        //then
        assertThat(row.get("key")).isNull();
    }

    @Test
    public void testEqualsAndHashcode() {
        // Given
        Row row1 = new Row();
        row1.put("column1", 1);
        row1.put("column2", "A");
        Row row2 = new Row();
        row2.put("column1", 1);
        row2.put("column2", "A");
        Row row3 = new Row();
        row3.put("column1", 1);
        row3.put("column2", "B");

        // When
        boolean test1 = row1.equals(row2);
        boolean test2 = row1.equals(row3);
        int hashCode1 = row1.hashCode();
        int hashCode2 = row2.hashCode();
        int hashCode3 = row3.hashCode();

        // Then
        assertThat(test1).isTrue();
        assertThat(test2).isFalse();
        assertThat(hashCode2).isEqualTo(hashCode1);
        assertThat(hashCode3).isNotEqualTo(hashCode1);
    }

    @Test
    public void testEqualsAndHashcodeWithAByteArrayValue() {
        // Given
        Row row1 = new Row();
        row1.put("column1", 1);
        row1.put("column2", new byte[]{1, 2});
        Row row2 = new Row();
        row2.put("column1", 1);
        row2.put("column2", new byte[]{1, 2});
        Row row3 = new Row();
        row3.put("column1", 1);
        row3.put("column2", new byte[]{1, 3});

        // When
        boolean test1 = row1.equals(row2);
        boolean test2 = row1.equals(row3);
        int hashCode1 = row1.hashCode();
        int hashCode2 = row2.hashCode();
        int hashCode3 = row3.hashCode();

        // Then
        assertThat(test1).isTrue();
        assertThat(test2).isFalse();
        assertThat(hashCode2).isEqualTo(hashCode1);
        assertThat(hashCode3).isNotEqualTo(hashCode1);
    }
}
