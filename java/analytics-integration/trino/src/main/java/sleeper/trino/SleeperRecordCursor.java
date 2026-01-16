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
package sleeper.trino;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;

import sleeper.trino.utils.SleeperPageBlockUtils;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * A record cursor that is provided with a stream of result rows and returns the rows one by one.
 */
public class SleeperRecordCursor implements RecordCursor {
    private static final Logger LOGGER = Logger.get(SleeperRecordCursor.class);

    private final String queryId;
    private final List<Type> columnTrinoTypesInOrder;
    private final Stream<List<Object>> resultRowStream;
    private final Iterator<List<Object>> resultRowIterator;
    private final int noOfColumns;

    private List<Object> currentRow = null;
    private long totalNoOfRowsReturned = 0L;

    /**
     * Creates a cursor from a stream of result rows. The rows will be returned by this cursor one by one. Each result
     * row is specified as a list of objects: the type of each object is provided as a separate argument.
     *
     * @param queryId                 the query ID, which is used to tag debug messages
     * @param columnTrinoTypesInOrder the types of the rows that are returned by this cursor, in the same order as the
     *                                fields that are returned
     * @param resultRowStream         the stream of rows for this cursor to return
     */
    public SleeperRecordCursor(
            String queryId,
            List<Type> columnTrinoTypesInOrder,
            Stream<List<Object>> resultRowStream) {
        this.queryId = requireNonNull(queryId);
        this.columnTrinoTypesInOrder = requireNonNull(columnTrinoTypesInOrder);
        this.resultRowStream = requireNonNull(resultRowStream);
        this.resultRowIterator = resultRowStream.iterator();
        this.noOfColumns = this.columnTrinoTypesInOrder.size();
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public Type getType(int fieldIndex) {
        checkArgument(fieldIndex < noOfColumns, "Invalid field index");
        return columnTrinoTypesInOrder.get(fieldIndex);
    }

    @Override
    public boolean advanceNextPosition() {
        if (!resultRowIterator.hasNext()) {
            currentRow = null;
            return false;
        }
        currentRow = resultRowIterator.next();
        if (totalNoOfRowsReturned == 0) {
            LOGGER.debug("Advanced to first row of record cursor %s", queryId);
        }
        totalNoOfRowsReturned++;
        return true;
    }

    @Override
    public boolean getBoolean(int fieldIndex) {
        checkFieldType(fieldIndex, ImmutableList.of(BOOLEAN));
        return (Boolean) currentRow.get(fieldIndex);
    }

    @Override
    public long getLong(int fieldIndex) {
        checkFieldType(fieldIndex, ImmutableList.of(BIGINT, INTEGER));
        Object value = currentRow.get(fieldIndex);
        if (value instanceof Integer) {
            return Long.valueOf((Integer) value);
        } else {
            return (Long) value;
        }
    }

    @Override
    public double getDouble(int fieldIndex) {
        checkFieldType(fieldIndex, ImmutableList.of(DOUBLE));
        return (Double) currentRow.get(fieldIndex);
    }

    @Override
    public Slice getSlice(int fieldIndex) {
        checkFieldType(fieldIndex, ImmutableList.of(VARCHAR));
        String str = (String) currentRow.get(fieldIndex);
        return Slices.utf8Slice(str);
    }

    /**
     * This method returns complex objects, such as arrays and maps. Support for these is currently experimental.
     *
     * @param  fieldIndex The index of the field to return.
     * @return            The contents of the field.
     */
    @Override
    public Object getObject(int fieldIndex) {
        Type fieldType = getType(fieldIndex);
        Object value = currentRow.get(fieldIndex);
        // This feels like problematic code and so watch for unexpected errors
        if (fieldType instanceof ArrayType) {
            List<?> valueAsList = (List<?>) value;
            VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 100, 10000);
            valueAsList.forEach(val -> SleeperPageBlockUtils.writeElementToBuilder(blockBuilder, (ArrayType) fieldType, val));
            return blockBuilder.build();
        } else {
            throw new UnsupportedOperationException(String.format("Complex objects of type %s are not supported", fieldType));
        }
    }

    @Override
    public boolean isNull(int fieldIndex) {
        checkArgument(fieldIndex < this.noOfColumns, "Invalid field index");
        return currentRow.get(fieldIndex) == null;
    }

    @Override
    public void close() {
        resultRowStream.close();
        LOGGER.debug("Record cursor for query %s returned %d rows", queryId, totalNoOfRowsReturned);
    }

    private void checkFieldType(int fieldIndex, List<Type> expectedTypes) {
        Type actualType = columnTrinoTypesInOrder.get(fieldIndex);
        if (expectedTypes.contains(actualType)) {
            return;
        }
        String expectedTypesString = Joiner.on(", ").join(expectedTypes);
        throw new IllegalArgumentException(format("Expected field %s to be type %s but is %s", fieldIndex, expectedTypesString, actualType));
    }
}
