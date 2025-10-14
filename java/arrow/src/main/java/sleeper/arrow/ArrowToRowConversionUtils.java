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

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.util.Text;

import sleeper.core.row.Row;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility functions to convert an Arrow batch of records into Sleeper rows.
 */
public class ArrowToRowConversionUtils {
    public static final String MAP_KEY_FIELD_NAME = "key";
    public static final String MAP_VALUE_FIELD_NAME = "value";

    private ArrowToRowConversionUtils() {
        throw new AssertionError();
    }

    /**
     * Constructs a Sleeper row from a single Arrow row.
     *
     * @param  vectorSchemaRoot          the container for all of the vectors which hold the values to use
     * @param  rowNo                     the index to read from each vector
     * @return                           a new row object holding those values
     * @throws IndexOutOfBoundsException if rowNo is out of bounds of the given vector
     */
    public static Row convertVectorSchemaRootToRow(VectorSchemaRoot vectorSchemaRoot, int rowNo) {
        if (rowNo < 0 || rowNo >= vectorSchemaRoot.getRowCount()) {
            throw new IndexOutOfBoundsException("Row number greater than number of rows " + rowNo + ">=" + vectorSchemaRoot.getRowCount());
        }
        int noOfFields = vectorSchemaRoot.getSchema().getFields().size();
        Row row = new Row();
        for (int fieldNo = 0; fieldNo < noOfFields; fieldNo++) {
            FieldVector fieldVector = vectorSchemaRoot.getVector(fieldNo);
            Object value = convertValueFromArrow(fieldVector, fieldVector.getObject(rowNo));
            row.put(fieldVector.getName(), value);
        }
        return row;
    }

    /**
     * Converts a given Arrow value to a Java value.
     * <p>
     * If the value is a primitive, it will remain unchanged. Instances of {@link Text} will be converted
     * to {@link String}. Maps and lists will be converted to Java {@link Map} and {@link List} instances.
     * <p>
     * This function will recurse into deeper nested structures.
     *
     * @param  fieldVector the column being converted
     * @param  value       value to convert
     * @return             plain Java value
     */
    private static Object convertValueFromArrow(FieldVector fieldVector, Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Text) {
            // The Parquet writer does not handle Text fields and so convert to a String
            return value.toString();
        } else if (fieldVector.getMinorType() == Types.MinorType.LIST) {
            // Arrow list fields may store genuine lists, or instead store a map as a list of structs
            boolean isActuallyMap = fieldVector.getChildrenFromFields().size() == 1 &&
                    fieldVector.getChildrenFromFields().get(0).getMinorType() == Types.MinorType.STRUCT &&
                    fieldVector.getChildrenFromFields().get(0).getChildrenFromFields().size() == 2 &&
                    fieldVector.getChildrenFromFields().get(0).getChildrenFromFields().get(0).getField().getName().equals(ArrowToRowConversionUtils.MAP_KEY_FIELD_NAME) &&
                    fieldVector.getChildrenFromFields().get(0).getChildrenFromFields().get(1).getField().getName().equals(ArrowToRowConversionUtils.MAP_VALUE_FIELD_NAME);
            if (isActuallyMap) {
                return arrowToMap(fieldVector, (List<?>) value);
            } else {
                // Convert any text elements into strings
                return ((List<?>) value).stream()
                        .map(v -> convertValueFromArrow(fieldVector.getChildrenFromFields().get(0), v))
                        .collect(Collectors.toList());
            }
        } else if (fieldVector.getMinorType() == Types.MinorType.MAP) {
            return arrowToMap(fieldVector, (List<?>) value);
        } else {
            return value;
        }
    }

    /**
     * Converts a Arrow map column to a Java map.
     *
     * @param  fieldVector the Arrow map vector
     * @param  value       Arrow map
     * @return             Java map instance
     */
    private static Map<Object, Object> arrowToMap(FieldVector fieldVector, List<?> value) {
        // Convert the list of structs into a map
        return value.stream()
                .map(obj -> (Map<?, ?>) obj)
                .map(map -> new AbstractMap.SimpleEntry<>(
                        map.get(MAP_KEY_FIELD_NAME),
                        map.get(MAP_VALUE_FIELD_NAME)))
                .collect(Collectors.toMap(
                        entry -> convertValueFromArrow(fieldVector.getChildrenFromFields().get(0).getChildrenFromFields().get(0), entry.getKey()),
                        entry -> convertValueFromArrow(fieldVector.getChildrenFromFields().get(0).getChildrenFromFields().get(1), entry.getValue())));
    }
}
