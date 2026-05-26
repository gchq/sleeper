/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.trino;

import com.google.common.collect.ImmutableList;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.trino.testutils.PopulatedSleeperExternalResource;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class NullableValueFieldIT {
    private static final String TEST_TABLE_NAME = "nullable_test";

    @RegisterExtension
    public static final PopulatedSleeperExternalResource POPULATED_SLEEPER_EXTERNAL_RESOURCE =
            new PopulatedSleeperExternalResource(ImmutableList.of(
                    new PopulatedSleeperExternalResource.TableDefinition(
                            TEST_TABLE_NAME,
                            Schema.builder()
                                    .rowKeyFields(new Field("key", new StringType()))
                                    .valueFields(new Field("value", new StringType(), true))
                                    .build(),
                            List.of(),
                            generateRows())));

    private static QueryAssertions assertions;

    private static Stream<Row> generateRows() {
        Row rowWithValue = new Row();
        rowWithValue.put("key", "present");
        rowWithValue.put("value", "hello");
        Row rowWithNull = new Row();
        rowWithNull.put("key", "absent");
        rowWithNull.put("value", null);
        return Stream.of(rowWithValue, rowWithNull);
    }

    @BeforeAll
    public static void beforeClass() {
        assertions = POPULATED_SLEEPER_EXTERNAL_RESOURCE.getQueryAssertions();
    }

    @Test
    public void shouldReturnValueWhenNullableFieldIsPresent() {
        assertThat(assertions.query(String.format(
                "SELECT key, value FROM sleeper.default.%s WHERE key = 'present'", TEST_TABLE_NAME)))
                .matches("VALUES (CAST('present' AS VARCHAR), CAST('hello' AS VARCHAR))");
    }

    @Test
    public void shouldReturnNullWhenNullableFieldIsAbsent() {
        assertThat(assertions.query(String.format(
                "SELECT key, value FROM sleeper.default.%s WHERE key = 'absent'", TEST_TABLE_NAME)))
                .matches("VALUES (CAST('absent' AS VARCHAR), CAST(NULL AS VARCHAR))");
    }

    @Test
    public void shouldFilterRowsWhereNullableFieldIsNull() {
        assertThat(assertions.query(String.format(
                "SELECT key FROM sleeper.default.%s WHERE value IS NULL AND key >= ''", TEST_TABLE_NAME)))
                .matches("VALUES (CAST('absent' AS VARCHAR))");
    }
}
