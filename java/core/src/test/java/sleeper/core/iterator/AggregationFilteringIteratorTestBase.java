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
package sleeper.core.iterator;

import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.util.IteratorConfig;
import sleeper.core.util.IteratorFactory;
import sleeper.core.util.ObjectFactory;

/**
 * Test base class for the generator methods around aggregation filters.
 */
public abstract class AggregationFilteringIteratorTestBase {

    protected static AggregationFilteringIterator createIterator(Schema schema, String filters, String aggregationString) throws IteratorCreationException {
        if (schema == null) {
            schema = generateDefaultTestAggregatorSchema();
        }
        return (AggregationFilteringIterator) new IteratorFactory(
                new ObjectFactory(AggregationFilteringIteratorTestBase.class.getClassLoader()))
                .getIterator(IteratorConfig.builder()
                        .filters(filters)
                        .aggregationString(aggregationString)
                        .build(), schema);
    }

    protected static AggregationFilteringIterator createIteratorWithAggregations() throws IteratorCreationException {
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key1", new IntType()), new Field("key2", new IntType()))
                .sortKeyFields(new Field("sort_key", new StringType()), new Field("sort_key2", new StringType()))
                .valueFields(new Field("value1", new LongType()), new Field("value2", new LongType()))
                .build();

        return (AggregationFilteringIterator) new IteratorFactory(
                new ObjectFactory(AggregationIteratorImplTest.class.getClassLoader()))
                .getIterator(IteratorConfig.builder()
                        .filters("")
                        .aggregationString("sum(value1),min(value2)")
                        .build(), schema);
    }

    private static Schema generateDefaultTestAggregatorSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key1", new IntType()), new Field("key2", new IntType()))
                .sortKeyFields(new Field("sort_key", new StringType()), new Field("sort_key2", new StringType()))
                .valueFields(new Field("value", new LongType()))
                .build();
    }
}
