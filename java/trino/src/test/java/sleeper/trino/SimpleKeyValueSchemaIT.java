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
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This does not currently test whether the execution plan is sensible or not.
 */
public class SimpleKeyValueSchemaIT {
    private static final String TEST_TABLE_NAME = "mytable";
    private static final int NO_OF_RECORDS = 1000;
    private static final List<PopulatedSleeperExternalResource.TableDefinition> TABLE_DEFINITIONS = ImmutableList.of(
            new PopulatedSleeperExternalResource.TableDefinition(
                    TEST_TABLE_NAME,
                    generateSimpleSchema(),
                    List.of(),
                    generateSimpleRecordStream()));

    @RegisterExtension
    public static final PopulatedSleeperExternalResource POPULATED_SLEEPER_EXTERNAL_RESOURCE = new PopulatedSleeperExternalResource(TABLE_DEFINITIONS);
    private static QueryAssertions assertions;

    private static Schema generateSimpleSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .valueFields(new Field("value", new StringType()))
                .build();
    }

    private static Stream<Row> generateSimpleRecordStream() {
        return IntStream.range(0, NO_OF_RECORDS).mapToObj(recordNo -> {
            Row row = new Row();
            row.put("key", String.format("key-%09d", recordNo));
            row.put("value", String.format("val-%09d", recordNo));
            return row;
        });
    }

    @BeforeAll
    public static void beforeClass() {
        assertions = POPULATED_SLEEPER_EXTERNAL_RESOURCE.getQueryAssertions();
    }

    @Test
    public void testEq() {
        assertThat(assertions.query(String.format(
                "SELECT key, value FROM sleeper.default.%s WHERE key = 'key-000000000'", TEST_TABLE_NAME)))
                .matches("VALUES (CAST ('key-000000000' AS VARCHAR), CAST('val-000000000' AS VARCHAR))");
    }

    @Test
    public void testEqColumnsReversed() {
        assertThat(assertions.query(String.format(
                "SELECT value, key FROM sleeper.default.%s WHERE key = 'key-000000000'", TEST_TABLE_NAME)))
                .matches("VALUES (CAST ('val-000000000' AS VARCHAR), CAST('key-000000000' AS VARCHAR))");
    }

    @Test
    public void testEqKeyOnly() {
        assertThat(assertions.query(String.format(
                "SELECT key FROM sleeper.default.%s WHERE key = 'key-000000000'", TEST_TABLE_NAME)))
                .matches("VALUES (CAST ('key-000000000' AS VARCHAR))");
    }

    @Test
    public void testEqValueOnly() {
        assertThat(assertions.query(String.format(
                "SELECT value FROM sleeper.default.%s WHERE key = 'key-000000000'", TEST_TABLE_NAME)))
                .matches("VALUES (CAST('val-000000000' AS VARCHAR))");
    }

    @Test
    public void testKeyEqAndValueEq() {
        assertThat(assertions.query(String.format(
                "SELECT key, value FROM sleeper.default.%s WHERE key = 'key-000000000' AND value = 'val-000000000'", TEST_TABLE_NAME)))
                .matches("VALUES (CAST ('key-000000000' AS VARCHAR), CAST('val-000000000' AS VARCHAR))");
    }

    @Test
    public void testKeyBetweenAndValueIn() {
        assertThat(assertions.query(String.format(
                "SELECT key, value FROM sleeper.default.%s " +
                        "WHERE key BETWEEN 'key-000000000' AND 'key-000000010' " +
                        "AND value IN ('val-000000001', 'val-000000002', 'val-000000099')",
                TEST_TABLE_NAME)))
                .matches("VALUES " +
                        "(CAST ('key-000000001' AS VARCHAR), CAST('val-000000001' AS VARCHAR)), " +
                        "(CAST ('key-000000002' AS VARCHAR), CAST('val-000000002' AS VARCHAR))");
    }

    @Test
    public void testGtEqLtEq() {
        assertThat(assertions.query(String.format(
                "SELECT key, value FROM sleeper.default.%s WHERE key >= 'key-000000000' AND key <= 'key-000000003'", TEST_TABLE_NAME)))
                .matches("VALUES " +
                        "(CAST ('key-000000000' AS VARCHAR), CAST('val-000000000' AS VARCHAR)), " +
                        "(CAST ('key-000000001' AS VARCHAR), CAST('val-000000001' AS VARCHAR)), " +
                        "(CAST ('key-000000002' AS VARCHAR), CAST('val-000000002' AS VARCHAR)), " +
                        "(CAST ('key-000000003' AS VARCHAR), CAST('val-000000003' AS VARCHAR))");
    }

    @Test
    public void testGtLt() {
        assertThat(assertions.query(String.format(
                "SELECT key, value FROM sleeper.default.%s WHERE key > 'key-000000000' AND key < 'key-000000003'", TEST_TABLE_NAME)))
                .matches("VALUES " +
                        "(CAST ('key-000000001' AS VARCHAR), CAST('val-000000001' AS VARCHAR)), " +
                        "(CAST ('key-000000002' AS VARCHAR), CAST('val-000000002' AS VARCHAR))");
    }

    @Test
    public void testBetween() {
        assertThat(assertions.query(String.format(
                "SELECT key, value FROM sleeper.default.%s WHERE key BETWEEN 'key-000000000' AND 'key-000000003'", TEST_TABLE_NAME)))
                .matches("VALUES " +
                        "(CAST ('key-000000000' AS VARCHAR), CAST('val-000000000' AS VARCHAR)), " +
                        "(CAST ('key-000000001' AS VARCHAR), CAST('val-000000001' AS VARCHAR)), " +
                        "(CAST ('key-000000002' AS VARCHAR), CAST('val-000000002' AS VARCHAR)), " +
                        "(CAST ('key-000000003' AS VARCHAR), CAST('val-000000003' AS VARCHAR))");
    }

    @Test
    public void testIn() {
        assertThat(assertions.query(String.format(
                "SELECT key, value FROM sleeper.default.%s WHERE key IN ('key-000000001', 'key-000000010', 'key-000000100')", TEST_TABLE_NAME)))
                .matches("VALUES " +
                        "(CAST ('key-000000001' AS VARCHAR), CAST('val-000000001' AS VARCHAR)), " +
                        "(CAST ('key-000000010' AS VARCHAR), CAST('val-000000010' AS VARCHAR)), " +
                        "(CAST ('key-000000100' AS VARCHAR), CAST('val-000000100' AS VARCHAR))");
    }

    @Test
    public void testLike() {
        assertThat(assertions.query(String.format(
                "SELECT key, value FROM sleeper.default.%s WHERE key LIKE 'key-00000000%%'", TEST_TABLE_NAME)))
                .matches("VALUES " +
                        "(CAST ('key-000000000' AS VARCHAR), CAST('val-000000000' AS VARCHAR)), " +
                        "(CAST ('key-000000001' AS VARCHAR), CAST('val-000000001' AS VARCHAR)), " +
                        "(CAST ('key-000000002' AS VARCHAR), CAST('val-000000002' AS VARCHAR)), " +
                        "(CAST ('key-000000003' AS VARCHAR), CAST('val-000000003' AS VARCHAR)), " +
                        "(CAST ('key-000000004' AS VARCHAR), CAST('val-000000004' AS VARCHAR)), " +
                        "(CAST ('key-000000005' AS VARCHAR), CAST('val-000000005' AS VARCHAR)), " +
                        "(CAST ('key-000000006' AS VARCHAR), CAST('val-000000006' AS VARCHAR)), " +
                        "(CAST ('key-000000007' AS VARCHAR), CAST('val-000000007' AS VARCHAR)), " +
                        "(CAST ('key-000000008' AS VARCHAR), CAST('val-000000008' AS VARCHAR)), " +
                        "(CAST ('key-000000009' AS VARCHAR), CAST('val-000000009' AS VARCHAR))");
    }

    @Test
    public void testCountMinMax() {
        assertThat(assertions.query(String.format(
                "SELECT MIN(key), MAX(key), MIN(value), MAX(value), COUNT(*) " +
                        "FROM sleeper.default.%s WHERE key LIKE 'key-%%'",
                TEST_TABLE_NAME)))
                .matches(String.format("VALUES (" +
                        "CAST ('key-%09d' AS VARCHAR), " +
                        "CAST ('key-%09d' AS VARCHAR), " +
                        "CAST ('val-%09d' AS VARCHAR), " +
                        "CAST ('val-%09d' AS VARCHAR), " +
                        "CAST (%d AS BIGINT))",
                        0, NO_OF_RECORDS - 1, 0, NO_OF_RECORDS - 1, NO_OF_RECORDS));
    }
}
