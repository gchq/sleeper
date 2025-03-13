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
package sleeper.core.record;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RecordTest {

    @Test
    public void testAnFieldCanBeRemoved() {
        //Given
        Record record = new Record();
        record.put("key", "value");

        //when
        record.remove("key");

        //then
        assertThat(record.get("key")).isNull();
    }

    @Test
    public void testAnNonExistentFieldCanBeRemoved() {
        //Given
        Record record = new Record();

        //when
        record.remove("key");

        //then
        assertThat(record.get("key")).isNull();
    }

    @Test
    public void testEqualsAndHashcode() {
        // Given
        Record record1 = new Record();
        record1.put("column1", 1);
        record1.put("column2", "A");
        Record record2 = new Record();
        record2.put("column1", 1);
        record2.put("column2", "A");
        Record record3 = new Record();
        record3.put("column1", 1);
        record3.put("column2", "B");

        // When
        boolean test1 = record1.equals(record2);
        boolean test2 = record1.equals(record3);
        int hashCode1 = record1.hashCode();
        int hashCode2 = record2.hashCode();
        int hashCode3 = record3.hashCode();

        // Then
        assertThat(test1).isTrue();
        assertThat(test2).isFalse();
        assertThat(hashCode2).isEqualTo(hashCode1);
        assertThat(hashCode3).isNotEqualTo(hashCode1);
    }

    @Test
    public void testEqualsAndHashcodeWithAByteArrayValue() {
        // Given
        Record record1 = new Record();
        record1.put("column1", 1);
        record1.put("column2", new byte[]{1, 2});
        Record record2 = new Record();
        record2.put("column1", 1);
        record2.put("column2", new byte[]{1, 2});
        Record record3 = new Record();
        record3.put("column1", 1);
        record3.put("column2", new byte[]{1, 3});

        // When
        boolean test1 = record1.equals(record2);
        boolean test2 = record1.equals(record3);
        int hashCode1 = record1.hashCode();
        int hashCode2 = record2.hashCode();
        int hashCode3 = record3.hashCode();

        // Then
        assertThat(test1).isTrue();
        assertThat(test2).isFalse();
        assertThat(hashCode2).isEqualTo(hashCode1);
        assertThat(hashCode3).isNotEqualTo(hashCode1);
    }
}
