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

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.util.Text;

import sleeper.core.record.Record;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ArrowToRecordConversionUtils {
    private ArrowToRecordConversionUtils() {
        throw new AssertionError();
    }

    /**
     * Construct a Sleeper record from a single Arrow row.
     *
     * @param  vectorSchemaRoot the container for all of the vectors which hold the values to use
     * @param  rowNo            the index to read from each vector
     * @return                  a new Record object holding those values
     */
    public static Record convertVectorSchemaRootToRecord(VectorSchemaRoot vectorSchemaRoot, int rowNo) {
        int noOfFields = vectorSchemaRoot.getSchema().getFields().size();
        Record record = new Record();
        for (int fieldNo = 0; fieldNo < noOfFields; fieldNo++) {
            FieldVector fieldVector = vectorSchemaRoot.getVector(fieldNo);
            Object value = fieldVector.getObject(rowNo);
            if (value instanceof Text) {
                // The Parquet writer does not handle Text fields and so convert to a String
                value = value.toString();
            }
            if (fieldVector.getMinorType() == Types.MinorType.LIST) {
                // Arrow list fields may store genuine lists, or instead store a map as a list of structs
                boolean isActuallyMap = fieldVector.getChildrenFromFields().size() == 1 &&
                        fieldVector.getChildrenFromFields().get(0).getMinorType() == Types.MinorType.STRUCT &&
                        fieldVector.getChildrenFromFields().get(0).getChildrenFromFields().size() == 2 &&
                        fieldVector.getChildrenFromFields().get(0).getChildrenFromFields().get(0).getField().getName().equals(ArrowRecordBatch.MAP_KEY_FIELD_NAME) &&
                        fieldVector.getChildrenFromFields().get(0).getChildrenFromFields().get(1).getField().getName().equals(ArrowRecordBatch.MAP_VALUE_FIELD_NAME);
                if (isActuallyMap) {
                    // Convert the list of structs into a map
                    value = ((List<?>) value).stream()
                            .map(obj -> (Map<?, ?>) obj)
                            .map(map -> new AbstractMap.SimpleEntry<>(
                                    map.get(ArrowRecordBatch.MAP_KEY_FIELD_NAME),
                                    map.get(ArrowRecordBatch.MAP_VALUE_FIELD_NAME)))
                            .collect(Collectors.toMap(
                                    entry -> (entry.getKey() instanceof Text) ? entry.getKey().toString() : entry.getKey(),
                                    entry -> (entry.getValue() instanceof Text) ? entry.getValue().toString() : entry.getValue()));
                } else {
                    // Convert any text elements into strings
                    value = ((List<?>) value).stream()
                            .map(v -> (v instanceof Text) ? v.toString() : v)
                            .collect(Collectors.toList());
                }
            }
            record.put(fieldVector.getName(), value);
        }
        return record;
    }
}
