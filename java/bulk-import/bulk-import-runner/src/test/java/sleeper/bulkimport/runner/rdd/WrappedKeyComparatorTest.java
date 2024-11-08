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
package sleeper.bulkimport.runner.rdd;

import org.junit.jupiter.api.Test;

import sleeper.core.key.Key;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.StringType;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The WrappedKeyComparator class is a wrapper for KeyComparator. Therefore the majority of the
 * functionality is tested in the unit test for KeyComparator, and there is only one simple test
 * here.
 */
public class WrappedKeyComparatorTest {

    @Test
    public void shouldCompareCorrectly() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key1", new IntType()), new Field("key2", new ByteArrayType()))
                .sortKeyFields(new Field("sort1", new StringType()), new Field("sort2", new ByteArrayType()), new Field("sort3", new StringType()))
                .valueFields(new Field("value", new StringType()))
                .build();
        Key key1 = createKey(1, new byte[]{10}, "A", new byte[]{56}, "X");
        Key key2 = createKey(1, new byte[]{10}, "A", new byte[]{56}, "Y");
        Key key3 = createKey(1, new byte[]{10}, "A", new byte[]{57}, "X");
        Key key4 = createKey(1, new byte[]{10}, "B", new byte[]{56}, "X");
        Key key5 = createKey(1, new byte[]{11}, "A", new byte[]{56}, "X");
        Key key6 = createKey(2, new byte[]{10}, "A", new byte[]{56}, "X");
        Key key7 = createKey(2, new byte[]{10}, "A", new byte[]{56}, "X");
        WrappedKeyComparator comparator = new WrappedKeyComparator(new SchemaSerDe().toJson(schema));

        // When
        int compare12 = comparator.compare(key1, key2);
        int compare23 = comparator.compare(key2, key3);
        int compare34 = comparator.compare(key3, key4);
        int compare45 = comparator.compare(key4, key5);
        int compare56 = comparator.compare(key5, key6);
        int compare67 = comparator.compare(key6, key7);

        // Then
        assertThat(compare12).isNegative();
        assertThat(compare23).isNegative();
        assertThat(compare34).isNegative();
        assertThat(compare45).isNegative();
        assertThat(compare56).isNegative();
        assertThat(compare67).isZero();
    }

    private static Key createKey(int k1, byte[] k2, String s1, byte[] s2, String s3) {
        List<Object> list = new ArrayList<>();
        list.add(k1);
        list.add(k2);
        list.add(s1);
        list.add(s2);
        list.add(s3);
        return Key.create(list);
    }
}
