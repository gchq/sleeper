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

import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.util.IteratorConfig;
import sleeper.core.util.IteratorFactory;
import sleeper.core.util.ObjectFactory;

import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

/**
 * Test base class for the generator methods around aggregation filters.
 */
public abstract class AggregationFilteringIteratorTestBase {

    protected static AggregationFilteringIterator createAggregationFilteringIterator(Schema schema, String filters, String aggregationString) throws IteratorCreationException {
        if (schema == null) {
            schema = createSchemaWithKey("key1", new IntType());
        }
        return (AggregationFilteringIterator) new IteratorFactory(
                new ObjectFactory(AggregationFilteringIteratorTestBase.class.getClassLoader()))
                .getIterator(IteratorConfig.builder()
                        .filters(filters)
                        .aggregationString(aggregationString)
                        .build(), schema);
    }
}
