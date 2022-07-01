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
    private static final Logger LOG = Logger.get(SleeperRecordCursor.class);

    private final String queryId;
    private final List<Type> columnTrinoTypesInOrder;
    private final Stream<List<Object>> resultRowStream;
    private final Iterator<List<Object>> resultRowIterator;
    private final int noOfColumns;

    private List<Object> currentRow = null;
    private long totalNoOfRowsReturned = 0L;

    /**
     * This {@link RecordCursor} is supplied with a {@link Stream} of result rows, which will be returned by this cursor
     * one by one. Each result row is specified as a list of objects: the type of each object is provided as a separate
     * argument.
     *
     * @param queryId                 The query ID, which is used to tag debug messages.
     * @param columnTrinoTypesInOrder The types of the rows that are returned by this cursor, in the same order as the
     *                                fields that are returned.
     * @param resultRowStream         The stream of rows for this cursor to return.
     */
    public SleeperRecordCursor(String queryId,
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
        checkArgument(fieldIndex < this.noOfColumns, "Invalid field index");
        return this.columnTrinoTypesInOrder.get(fieldIndex);
    }

    @Override
    public boolean advanceNextPosition() {
        if (!this.resultRowIterator.hasNext()) {
            this.currentRow = null;
            return false;
        }
        this.currentRow = this.resultRowIterator.next();
        if (this.totalNoOfRowsReturned == 0) {
            LOG.debug("Advanced to first row of record cursor %s", queryId);
        }
        this.totalNoOfRowsReturned++;
        return true;
    }

    @Override
    public boolean getBoolean(int fieldIndex) {
        checkFieldType(fieldIndex, ImmutableList.of(BOOLEAN));
        return (Boolean) this.currentRow.get(fieldIndex);
    }

    @Override
    public long getLong(int fieldIndex) {
        checkFieldType(fieldIndex, ImmutableList.of(BIGINT, INTEGER));
        Object value = this.currentRow.get(fieldIndex);
        if (value instanceof Integer) {
            return Long.valueOf((Integer) value);
        } else {
            return (Long) value;
        }
    }

    @Override
    public double getDouble(int fieldIndex) {
        checkFieldType(fieldIndex, ImmutableList.of(DOUBLE));
        return (Double) this.currentRow.get(fieldIndex);
    }

    @Override
    public Slice getSlice(int fieldIndex) {
        checkFieldType(fieldIndex, ImmutableList.of(VARCHAR));
        String str = (String) this.currentRow.get(fieldIndex);
        return Slices.utf8Slice(str);
    }

    /**
     * This method returns complex objects, such as arrays and maps. Support for these is currently experimental.
     *
     * @param fieldIndex The index of the field to return.
     * @return The contents of the field.
     */
    @Override
    public Object getObject(int fieldIndex) {
        Type fieldType = this.getType(fieldIndex);
        Object value = this.currentRow.get(fieldIndex);
        // This feels like problematic code and so watch for unexpected errors
        if (fieldType instanceof ArrayType) {
            Type elementType = ((ArrayType) fieldType).getElementType();
            List<?> valueAsList = (List<?>) value;
            VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 100, 10000);
            valueAsList.forEach(val -> SleeperPageBlockUtils.writeElementToBuilder(blockBuilder, elementType, val));
            return blockBuilder.build();
        } else {
            throw new UnsupportedOperationException(String.format("Complex objects of type %s are not supported", fieldType));
        }
    }

    @Override
    public boolean isNull(int fieldIndex) {
        checkArgument(fieldIndex < this.noOfColumns, "Invalid field index");
        return this.currentRow.get(fieldIndex) == null;
    }

    @Override
    public void close() {
        this.resultRowStream.close();
        LOG.debug("Record cursor for query %s returned %d rows", this.queryId, this.totalNoOfRowsReturned);
    }

    private void checkFieldType(int fieldIndex, List<Type> expectedTypes) {
        Type actualType = this.columnTrinoTypesInOrder.get(fieldIndex);
        if (expectedTypes.contains(actualType)) {
            return;
        }
        String expectedTypesString = Joiner.on(", ").join(expectedTypes);
        throw new IllegalArgumentException(format("Expected field %s to be type %s but is %s", fieldIndex, expectedTypesString, actualType));
    }
}
