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

import sleeper.core.iterator.closeable.WrappedIterator;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.util.ObjectFactory;

import java.util.ArrayList;
import java.util.List;

public abstract class SortedRowIteratorTestBase {

    protected static List<Row> apply(SortedRowIterator iterator, List<Row> rows) {
        List<Row> output = new ArrayList<>();
        iterator.apply(new WrappedIterator<>(rows.iterator()))
                .forEachRemaining(output::add);
        return output;
    }

    protected static SortedRowIterator createIterator(IteratorConfig config, Schema schema) throws Exception {
        return new IteratorFactory(
                new ObjectFactory(IteratorFactoryTest.class.getClassLoader()))
                .getIterator(config, schema);
    }
}
