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
package sleeper.systemtest.datageneration;

import org.junit.jupiter.api.Test;

import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.systemtest.configuration.SystemTestRandomDataSettings;

import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class RandomRowSupplierTest {

    private static final int SAMPLE_SIZE = 1000;

    @Test
    void shouldNeverGenerateNullForNonNullableField() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .valueFields(new Field("value", new StringType()))
                .build();
        RandomRowSupplier supplier = new RandomRowSupplier(schema, SystemTestRandomDataSettings.fromDefaults());

        // When
        List<Row> rows = generateRows(supplier);

        // Then
        assertThat(rows).allSatisfy(row -> assertThat(row.get("value")).isNotNull());
    }

    @Test
    void shouldGenerateNullApproximately20PercentOfTimeForNullableField() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .valueFields(new Field("value", new StringType(), true))
                .build();
        RandomRowSupplier supplier = new RandomRowSupplier(schema, SystemTestRandomDataSettings.fromDefaults());

        // When
        List<Row> rows = generateRows(supplier);

        // Then
        long nullCount = rows.stream().filter(row -> row.get("value") == null).count();
        double nullFraction = (double) nullCount / SAMPLE_SIZE;
        assertThat(nullFraction).isBetween(0.10, 0.30);
    }

    @Test
    void shouldNeverGenerateNullForRowKeyField() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .valueFields(new Field("value", new StringType(), true))
                .build();
        RandomRowSupplier supplier = new RandomRowSupplier(schema, SystemTestRandomDataSettings.fromDefaults());

        // When
        List<Row> rows = generateRows(supplier);

        // Then
        assertThat(rows).allSatisfy(row -> assertThat(row.get("key")).isNotNull());
    }

    private static List<Row> generateRows(RandomRowSupplier supplier) {
        return IntStream.range(0, SAMPLE_SIZE)
                .mapToObj(i -> supplier.get())
                .toList();
    }
}
