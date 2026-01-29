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
package sleeper.splitter.core.sketches;

import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;

/**
 * Finds split points during partition splitting based on sketches. We avoid referencing the sketches directly so that
 * we can split a sketch into two while still reading the original sketch, when extending a partition tree over multiple
 * levels.
 */
public interface SketchesForSplitting {

    /**
     * Retrieves the sketch for the given row key field.
     *
     * @param  rowKeyField the field we want to split the partition on
     * @return             the sketch of data in that field
     */
    SketchForSplitting getSketch(Field rowKeyField);

    /**
     * Retrieves the number of records that were used to generate the sketches. Assumes that all the sketches include
     * the same records, and checks the sketch for the first row key field (dimension 0).
     *
     * @param  schema the Sleeper table schema
     * @return        the number of rows used to generate the sketch
     */
    default long getNumberOfRecordsSketched(Schema schema) {
        Field firstRowKeyField = schema.getRowKeyFields().get(0);
        return getSketch(firstRowKeyField).getNumberOfRows();
    }

}
