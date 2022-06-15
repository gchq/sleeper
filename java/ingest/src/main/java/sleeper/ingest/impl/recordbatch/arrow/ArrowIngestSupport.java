package sleeper.ingest.impl.recordbatch.arrow;

import org.apache.arrow.algorithm.sort.CompositeVectorComparator;
import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
import org.apache.arrow.algorithm.sort.IndexSorter;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.core.schema.type.*;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class ArrowIngestSupport {
    private static final Logger LOGGER = LoggerFactory.getLogger(ArrowIngestSupport.class);

    /**
     * Generate an {@link IntVector} which contains the indices of the rows of the {@link VectorSchemaRoot}, once the
     * rows have been sorted according to the row keys and sort keys specified in the supplied {@link
     * sleeper.core.schema.Schema}. The {@link VectorSchemaRoot} is unchanged on return.
     * <p>
     * In order to sort a {@link VectorSchemaRoot}, use this method to generate a sort order and then read the sorted
     * data out of the {@link VectorSchemaRoot} by reading vectorschemaroot(intvector(0)), then
     * vectorschemaroot(intvector(1)) and so on.
     * <p>
     * The caller must close the returned vector once it is no longer needed.
     */
    public static IntVector createSortOrderVector(BufferAllocator bufferAllocator,
                                                  sleeper.core.schema.Schema sleeperSchema,
                                                  VectorSchemaRoot vectorSchemaRoot) {
        // Work out which field is to be used for the sort, where it is in the fields, and what type it is
        int vectorSize = vectorSchemaRoot.getRowCount();
        List<sleeper.core.schema.Field> allSleeperFields = sleeperSchema.getAllFields();
        List<sleeper.core.schema.Field> sleeperSortOrderFieldsInOrder =
                Stream.of(sleeperSchema.getRowKeyFields(), sleeperSchema.getSortKeyFields())
                        .flatMap(List::stream)
                        .collect(Collectors.toList());
        List<VectorValueComparator<?>> vectorValueComparatorsInOrder = sleeperSortOrderFieldsInOrder.stream()
                .map(field -> {
                    Type fieldType = field.getType();
                    int indexOfField = allSleeperFields.indexOf(field);
                    if (fieldType instanceof IntType) {
                        VectorValueComparator<IntVector> vectorValueComparator = new DefaultVectorComparators.IntComparator();
                        vectorValueComparator.attachVector((IntVector) vectorSchemaRoot.getVector(indexOfField));
                        return vectorValueComparator;
                    } else if (fieldType instanceof LongType) {
                        VectorValueComparator<BigIntVector> vectorValueComparator = new DefaultVectorComparators.LongComparator();
                        vectorValueComparator.attachVector((BigIntVector) vectorSchemaRoot.getVector(indexOfField));
                        return vectorValueComparator;
                    } else if (fieldType instanceof StringType) {
                        VectorValueComparator<BaseVariableWidthVector> vectorValueComparator = new DefaultVectorComparators.VariableWidthComparator();
                        vectorValueComparator.attachVector((VarCharVector) vectorSchemaRoot.getVector(indexOfField));
                        return vectorValueComparator;
                    } else if (fieldType instanceof ByteArrayType) {
                        VectorValueComparator<BaseVariableWidthVector> vectorValueComparator = new DefaultVectorComparators.VariableWidthComparator();
                        vectorValueComparator.attachVector((VarBinaryVector) vectorSchemaRoot.getVector(indexOfField));
                        return vectorValueComparator;
                    } else {
                        throw new UnsupportedOperationException("Sleeper column type " + fieldType.toString() + " is not handled");
                    }
                }).collect(Collectors.toList());
        CompositeVectorComparator compositeVectorComparator = new CompositeVectorComparator(vectorValueComparatorsInOrder.toArray(new VectorValueComparator[0]));
        // Create a vector to hold the row indices of the data before it has been sorted and populate it with the
        // values 0...vectorSize. This will be sorted in the order specified by the CompositeVectorComparator
        // and so we create a vector to hold the row indices of the data once it has been sorted
        IntVector sortOrderVector = new IntVector("Sort order vector", bufferAllocator);
        try {
            sortOrderVector.allocateNew(vectorSize);
            sortOrderVector.setValueCount(vectorSize);
            IndexSorter<ValueVector> indexSorter = new IndexSorter<>();
            indexSorter.sort(vectorSchemaRoot.getVector(0), sortOrderVector, compositeVectorComparator);
            return sortOrderVector;
        } catch (Exception e) {
            sortOrderVector.close();
            throw e;
        }
    }
}
 