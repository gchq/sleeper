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
package sleeper.core.iterator.testutil;

import sleeper.core.iterator.IteratorConfig;
import sleeper.core.iterator.IteratorFactory;
import sleeper.core.iterator.SortedRowIterator;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.util.ObjectFactory;

/**
 * Helpers for working with IteratorFactory.
 */
public class IteratorFactoryTestHelper {

    private IteratorFactoryTestHelper() {
    }

    /**
     * Creates the default iterator for a Sleeper table, which is applied during compaction.
     *
     * @param  tableProperties the table properties
     * @return                 the iterator
     * @throws Exception       if the iterator could not be created
     */
    public static SortedRowIterator createIterator(TableProperties tableProperties) throws Exception {
        return createIterator(tableProperties, true, true);
    }

    /**
     * Creates the default iterator for a Sleeper table, which is applied during compaction.
     *
     * @param  tableProperties   the table properties
     * @param  applyFilters      if filtering iterators should be added
     * @param  applyAggregations if aggregating iterators should be added
     * @return                   the iterator
     * @throws Exception         if the iterator could not be created
     */
    public static SortedRowIterator createIterator(TableProperties tableProperties, boolean applyFilters, boolean applyAggregations) throws Exception {
        return new IteratorFactory(
                new ObjectFactory(IteratorFactoryTestHelper.class.getClassLoader()))
                .getIterator(IteratorConfig.from(tableProperties), tableProperties.getSchema(), applyFilters, applyAggregations);
    }
}
