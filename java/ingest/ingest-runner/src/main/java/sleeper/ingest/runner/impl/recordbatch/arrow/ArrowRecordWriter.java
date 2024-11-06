/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.ingest.runner.impl.recordbatch.arrow;

import org.apache.arrow.vector.VectorSchemaRoot;

import sleeper.core.schema.Field;

import java.util.List;

public interface ArrowRecordWriter<T> {

    /**
     * An interface which supports writing Arrow records. The interface allows the source data to come in different
     * formats, including those where each insert generates multiple Arrow records.
     *
     * @param  allFields        A List of all of the fields to store
     * @param  vectorSchemaRoot The Arrow in-memory store to store the records in
     * @param  data             The data to write
     * @param  insertAtRowNo    The location in the VectorSchemaRoot to use to insert the data
     * @return                  The index to use when this method is next called
     */
    int insert(List<Field> allFields, VectorSchemaRoot vectorSchemaRoot, T data, int insertAtRowNo);
}
