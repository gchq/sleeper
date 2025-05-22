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
package sleeper.query.lambda;

import org.junit.jupiter.api.Test;

import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class WarmQueryExecutorLambdaTest {

    @Test
    public void testStringTypeRegionIsReturned() {
        Schema schema = getStringKeySchema();
        Region region = WarmQueryExecutorLambda.getRegion(schema);

        assertThat(region).isEqualTo(getExpectedRegion(schema, new Field("test-key", new StringType()), "a"));
    }

    @Test
    public void testByteTypeRegionIsReturned() {
        Schema schema = getByteArrayKeySchema();
        Region region = WarmQueryExecutorLambda.getRegion(schema);

        assertThat(region).isEqualTo(getExpectedRegion(schema, new Field("test-key", new ByteArrayType()), new byte[]{'a'}));
    }

    @Test
    public void testIntTypeRegionIsReturned() {
        Schema schema = getIntKeySchema();
        Region region = WarmQueryExecutorLambda.getRegion(schema);

        assertThat(region).isEqualTo(getExpectedRegion(schema, new Field("test-key", new IntType()), 0));
    }

    @Test
    public void testLongTypeRegionIsReturned() {
        Schema schema = getLongKeySchema();
        Region region = WarmQueryExecutorLambda.getRegion(schema);

        assertThat(region).isEqualTo(getExpectedRegion(schema, new Field("test-key", new LongType()), 0L));
    }

    @Test
    public void testMultipleKeysInRegionAreReturned() {
        Schema schema = getMultipleKeySchema();
        Region region = WarmQueryExecutorLambda.getRegion(schema);

        Region expected = new Region(List.of(getExpectedRegion(schema, new Field("test-key", new StringType()), "a")
                .getRange("test-key"),
                getExpectedRegion(schema, new Field("test-key2", new StringType()), "a")
                        .getRange("test-key2")));

        assertThat(region).isEqualTo(expected);
    }

    private Schema getStringKeySchema() {
        return Schema.builder()
                .rowKeyFields(new Field("test-key", new StringType()))
                .sortKeyFields(new Field("test-sort", new StringType()))
                .valueFields(new Field("test-value", new StringType()))
                .build();
    }

    private Schema getMultipleKeySchema() {
        return Schema.builder()
                .rowKeyFields(List.of(new Field("test-key", new StringType()),
                        new Field("test-key2", new StringType())))
                .sortKeyFields(new Field("test-sort", new StringType()))
                .valueFields(new Field("test-value", new StringType()))
                .build();
    }

    private Schema getByteArrayKeySchema() {
        return Schema.builder()
                .rowKeyFields(new Field("test-key", new ByteArrayType()))
                .sortKeyFields(new Field("test-sort", new ByteArrayType()))
                .valueFields(new Field("test-value", new ByteArrayType()))
                .build();
    }

    private Schema getIntKeySchema() {
        return Schema.builder()
                .rowKeyFields(new Field("test-key", new IntType()))
                .sortKeyFields(new Field("test-sort", new IntType()))
                .valueFields(new Field("test-value", new IntType()))
                .build();
    }

    private Schema getLongKeySchema() {
        return Schema.builder()
                .rowKeyFields(new Field("test-key", new LongType()))
                .sortKeyFields(new Field("test-sort", new LongType()))
                .valueFields(new Field("test-value", new LongType()))
                .build();
    }

    private Region getExpectedRegion(Schema schema, Field rowKey, Object value) {
        return new Region(List.of(new Range.RangeFactory(schema)
                .createExactRange(rowKey, value)));
    }
}
