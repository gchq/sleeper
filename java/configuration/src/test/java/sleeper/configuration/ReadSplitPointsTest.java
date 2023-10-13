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

package sleeper.configuration;

import org.apache.commons.codec.binary.Base64;
import org.junit.jupiter.api.Test;

import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class ReadSplitPointsTest {

    @Test
    void shouldReadIntegerSplitPoints() {
        Schema schema = schemaWithKey("key", new IntType());
        String splitPoints = "1\n2\n3";

        assertThat(readSplitPoints(schema, splitPoints))
                .containsExactly(1, 2, 3);
    }

    @Test
    void shouldReadLongSplitPoints() {
        Schema schema = schemaWithKey("key", new LongType());
        String splitPoints = "1\n2\n3";

        assertThat(readSplitPoints(schema, splitPoints))
                .containsExactly(1L, 2L, 3L);
    }

    @Test
    void shouldReadStringSplitPoints() {
        Schema schema = schemaWithKey("key", new StringType());
        String splitPoints = "1\n2\n3";

        assertThat(readSplitPoints(schema, splitPoints))
                .containsExactly("1", "2", "3");
    }

    @Test
    void shouldReadByteArraySplitPointsAsBase64() {
        Schema schema = schemaWithKey("key", new ByteArrayType());
        String splitPoints = "AQ==\nAg==\nAw==";

        assertThat(readSplitPoints(schema, splitPoints))
                .containsExactly(new byte[]{1}, new byte[]{2}, new byte[]{3});
    }

    @Test
    void shouldEncodeBase64Data() {
        // Compare this against the previous test to confirm Base64 strings are correct
        assertThat(List.of(new byte[]{1}, new byte[]{2}, new byte[]{3}))
                .extracting(Base64::encodeBase64String)
                .containsExactly("AQ==", "Ag==", "Aw==");
    }

    @Test
    void shouldReadStringSplitPointsBase64Encoded() {
        Schema schema = schemaWithKey("key", new StringType());
        String splitPoints = "MQ==\nMg==\nMw==";

        assertThat(readSplitPointsWithBase64Strings(schema, splitPoints))
                .containsExactly("1", "2", "3");
    }

    @Test
    void shouldEncodeBase64DataFromStrings() {
        // Compare this against the previous test to confirm Base64 strings are correct
        assertThat(List.of("1", "2", "3"))
                .extracting(str -> Base64.encodeBase64String(str.getBytes(StandardCharsets.UTF_8)))
                .containsExactly("MQ==", "Mg==", "Mw==");
    }

    public static List<Object> readSplitPoints(Schema schema, String splitPointsString) {
        return ReadSplitPoints.fromString(splitPointsString, schema, false);
    }

    public static List<Object> readSplitPointsWithBase64Strings(Schema schema, String splitPointsString) {
        return ReadSplitPoints.fromString(splitPointsString, schema, true);
    }
}
