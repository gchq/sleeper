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

/**
 * A function to transform an iterator of sorted rows. For example, this may exclude some rows, perform an aggregation,
 * or perform some computation on the values to produce or remove fields. This is called an iterator because it creates
 * a Java Iterator, as an implementation to operate on rows as it iterates through them. This is also the name used in
 * Accumulo. It takes an input iterator, and creates an output iterator that may apply a transformation to the rows.
 * <p>
 * The iterator should respect the general constraints of a compaction. There could be many hundreds of millions of rows
 * processed by a single compaction job, so there should be no attempt to buffer lots of rows in memory. There is no
 * guarantee of the order the files in a partition will be compacted, or that all of them will be compacted at the same
 * time so the logic should be commutative and associative. The output should be sorted by key so in general the row and
 * sort keys should not be changed by the iterator.
 */
public interface ConfigStringIterator extends SortedRowIterator {

    /**
     * Configures the iterator to accept rows.
     *
     * @param configString configuration specific to the iterator which may be set before the iterator is used
     * @param schema       the schema of the Sleeper table being processed
     */
    void init(String configString, Schema schema);

}
