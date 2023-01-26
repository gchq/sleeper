/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.ingest.impl.recordbatch.arrow;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import sleeper.core.record.Record;

import static sleeper.arrow.record.RecordConverter.getValueFromFieldVector;

public class ArrowToRecordConversionUtils {
    private ArrowToRecordConversionUtils() {
        throw new AssertionError();
    }

    /**
     * Construct a {@link Record} object from a single row within a {@link VectorSchemaRoot}.
     *
     * @param vectorSchemaRoot The container for all of the vectors which hold the values to use
     * @param rowNo            The index to read from each vector
     * @return A new Record object holding those values
     */
    public static Record convertVectorSchemaRootToRecord(VectorSchemaRoot vectorSchemaRoot, int rowNo) {
        Record record = new Record();
        for (FieldVector fieldVector : vectorSchemaRoot.getFieldVectors()) {
            record.put(fieldVector.getName(), getValueFromFieldVector(fieldVector, rowNo));
        }
        return record;
    }
}
