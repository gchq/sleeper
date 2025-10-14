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
package sleeper.arrow;

import org.apache.arrow.vector.VectorSchemaRoot;

import sleeper.core.schema.Field;

import java.util.List;

/**
 * A writer to add data to an Arrow record batch. Allows the source data to come in different formats, including those
 * where each insert generates multiple Arrow rows.
 *
 * @param <T> source type of data to write
 */
public interface ArrowRowWriter<T> {

    /**
     * Adds data at a specific position in an Arrow record batch.
     *
     * @param  allFields        a list of all of the fields to store
     * @param  vectorSchemaRoot the Arrow in-memory store to store the rows in
     * @param  data             the data to write
     * @param  insertAtRowNo    the row number to write to in the vector
     * @return                  the next row number after the last row that was written
     */
    int insert(List<Field> allFields, VectorSchemaRoot vectorSchemaRoot, T data, int insertAtRowNo);
}
