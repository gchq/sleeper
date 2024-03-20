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
package sleeper.ingest.impl.recordbatch.arrow;

import org.apache.arrow.algorithm.sort.CompositeVectorComparator;
import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
import org.apache.arrow.algorithm.sort.IndexSorter;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class ArrowIngestSupport {

    private ArrowIngestSupport() {
    }

    /**
     * Create a vector of row indices, sorted according to a Sleeper schema. Generates an {@link IntVector} which
     * contains the indices of the rows of the {@link VectorSchemaRoot}. The indices will be sorted according to the row
     * keys and sort keys specified in the supplied {@link sleeper.core.schema.Schema}. The {@link VectorSchemaRoot} is
     * unchanged on return.
     * <p>
     * In order to sort a {@link VectorSchemaRoot}, use this method to generate a sort order and then read the sorted
     * data out of the {@link VectorSchemaRoot} by reading vectorschemaroot(intvector(0)), then
     * vectorschemaroot(intvector(1)) and so on.
     * <p>
     * The caller must close the returned vector once it is no longer needed.
     *
     * @param  bufferAllocator  allocator for the sort order vector
     * @param  sleeperSchema    schema to use to sort by its keys
     * @param  vectorSchemaRoot vector to sort
     * @return                  the sort order
     */
    public static IntVector createSortOrderVector(BufferAllocator bufferAllocator,
            sleeper.core.schema.Schema sleeperSchema,
            VectorSchemaRoot vectorSchemaRoot) {
        // Work out which field is to be used for the sort, where it is in the fields, and what type it is
        int vectorSize = vectorSchemaRoot.getRowCount();
        List<sleeper.core.schema.Field> allSleeperFields = sleeperSchema.getAllFields();
        List<sleeper.core.schema.Field> sleeperSortOrderFieldsInOrder = Stream.of(sleeperSchema.getRowKeyFields(), sleeperSchema.getSortKeyFields())
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
