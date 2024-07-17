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

package sleeper.configuration;

import com.facebook.collections.ByteArray;
import org.junit.jupiter.api.Test;

import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class ReadWriteSplitPointsTest {

    @Test
    void shouldReadIntegerSplitPoints() {
        // Given
        Schema schema = schemaWithKey("key", new IntType());
        List<Object> splitPoints = List.of(1, 2, 3);

        // When
        String splitPointsStr = writeSplitPoints(splitPoints);
        List<Object> splitPointsRead = readSplitPoints(schema, splitPointsStr);

        // Then
        assertThat(splitPointsRead).isEqualTo(splitPoints);
        assertThat(splitPointsStr).isEqualTo("1\n2\n3\n");
    }

    @Test
    void shouldReadLongSplitPoints() {
        // Given
        Schema schema = schemaWithKey("key", new LongType());
        List<Object> splitPoints = List.of(1L, 2L, 3L);

        // When
        String splitPointsStr = writeSplitPoints(splitPoints);
        List<Object> splitPointsRead = readSplitPoints(schema, splitPointsStr);

        // Then
        assertThat(splitPointsRead).isEqualTo(splitPoints);
        assertThat(splitPointsStr).isEqualTo("1\n2\n3\n");
    }

    @Test
    void shouldReadStringSplitPoints() {
        // Given
        Schema schema = schemaWithKey("key", new StringType());
        List<Object> splitPoints = List.of("1", "2", "3");

        // When
        String splitPointsStr = writeSplitPoints(splitPoints);
        List<Object> splitPointsRead = readSplitPoints(schema, splitPointsStr);

        // Then
        assertThat(splitPointsRead).isEqualTo(splitPoints);
        assertThat(splitPointsStr).isEqualTo("1\n2\n3\n");
    }

    @Test
    void shouldReadByteArraySplitPointsAsBase64() {
        Schema schema = schemaWithKey("key", new ByteArrayType());
        List<Object> splitPoints = List.of(new byte[]{1}, new byte[]{2}, new byte[]{3});

        // When
        String splitPointsStr = writeSplitPoints(splitPoints);
        List<Object> splitPointsRead = readSplitPoints(schema, splitPointsStr);

        // Then
        assertThat(splitPointsRead).containsExactlyElementsOf(splitPoints);
        assertThat(splitPointsStr).isEqualTo("AQ==\nAg==\nAw==\n");
    }

    @Test
    void shouldReadWrappedByteArraySplitPointsAsBase64() {
        Schema schema = schemaWithKey("key", new ByteArrayType());
        List<Object> splitPoints = List.of(new byte[]{1}, new byte[]{2}, new byte[]{3});
        List<Object> wrappedSplitPoints = List.of(ByteArray.wrap(new byte[]{1}), ByteArray.wrap(new byte[]{2}), ByteArray.wrap(new byte[]{3}));

        // When
        String splitPointsStr = writeSplitPoints(wrappedSplitPoints);
        List<Object> splitPointsRead = readSplitPoints(schema, splitPointsStr);

        // Then
        assertThat(splitPointsRead).containsExactlyElementsOf(splitPoints);
        assertThat(splitPointsStr).isEqualTo("AQ==\nAg==\nAw==\n");
    }

    @Test
    void shouldReadStringSplitPointsBase64Encoded() {
        // Given
        Schema schema = schemaWithKey("key", new StringType());
        List<Object> splitPoints = List.of("1", "2", "3");

        // When
        String splitPointsStr = writeSplitPointsWithBase64Strings(splitPoints);
        List<Object> splitPointsRead = readSplitPointsWithBase64Strings(schema, splitPointsStr);

        // Then
        assertThat(splitPointsRead).isEqualTo(splitPoints);
        assertThat(splitPointsStr).isEqualTo("MQ==\nMg==\nMw==\n");
    }

    public static String writeSplitPoints(List<Object> splitPoints) {
        return WriteSplitPoints.toString(splitPoints, false);
    }

    public static String writeSplitPointsWithBase64Strings(List<Object> splitPoints) {
        return WriteSplitPoints.toString(splitPoints, true);
    }

    public static List<Object> readSplitPoints(Schema schema, String splitPointsString) {
        return ReadSplitPoints.fromString(splitPointsString, schema, false);
    }

    public static List<Object> readSplitPointsWithBase64Strings(Schema schema, String splitPointsString) {
        return ReadSplitPoints.fromString(splitPointsString, schema, true);
    }
}
