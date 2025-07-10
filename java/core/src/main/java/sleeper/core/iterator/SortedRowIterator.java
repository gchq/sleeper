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

import sleeper.core.row.Row;
import sleeper.core.schema.Schema;

import java.util.List;
import java.util.function.Function;

/**
 * A function to transform an iterator of rows. For example, this may exclude some rows, perform an aggregation,
 * or perform some computation on the values to produce or remove fields.
 */
public interface SortedRowIterator extends Function<CloseableIterator<Row>, CloseableIterator<Row>> {

    /**
     * Configures the iterator to accept rows.
     *
     * @param configString configuration specific to the iterator which may be set before the iterator is used
     * @param schema       the schema of the Sleeper table being processed
     */
    void init(String configString, Schema schema);

    /**
     * This should provide a list of fields which will be read by the iterator. This is to ensure that those fields will
     * be read from Sleeper, even if a client requested a limited set of fields which does not include them.
     *
     * @return names of fields that must be loaded
     */
    List<String> getRequiredValueFields();
}
