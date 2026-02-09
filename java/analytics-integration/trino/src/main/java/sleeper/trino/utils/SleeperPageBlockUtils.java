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
package sleeper.trino.utils;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import sleeper.trino.handle.SleeperColumnHandle;

import java.io.UncheckedIOException;
import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class SleeperPageBlockUtils {

    private SleeperPageBlockUtils() {
    }

    /**
     * Read a single object from the specified channel and position in the given page.
     *
     * @param  sleeperColumnHandlesInChannelOrder the handles for each column stored in the page, in the order of the
     *                                            channels in the page
     * @param  page                               the page to read from
     * @param  channelNo                          the channel to read
     * @param  positionNo                         the position to read
     * @return                                    the read object
     */
    public static Object readObjectFromPage(
            List<SleeperColumnHandle> sleeperColumnHandlesInChannelOrder,
            Page page,
            int channelNo,
            int positionNo) {
        Type trinoType = sleeperColumnHandlesInChannelOrder.get(channelNo).getColumnTrinoType();
        Block block = page.getBlock(channelNo);
        if (trinoType.equals(IntegerType.INTEGER)) {
            return block.getInt(positionNo, 0);
        }
        if (trinoType.equals(BigintType.BIGINT)) {
            return block.getLong(positionNo, 0);
        }
        if (trinoType.equals(VarcharType.VARCHAR)) {
            return block.getSlice(positionNo, 0, block.getSliceLength(positionNo)).toStringUtf8();
        }
        throw new UnsupportedOperationException(String.format("Trino type %s cannot be read from a page", trinoType.getBaseName()));
    }

    /**
     * Write an element into a block builder. This method is used when columns of type {@link ArrayType} are
     * being processed, which is experimental at present.
     *
     * @param blockBuilder the block to write the elements into
     * @param fieldType    the type of the field being written to
     * @param element      the element itself
     */
    public static void writeElementToBuilder(VariableWidthBlockBuilder blockBuilder, ArrayType fieldType, Object element) {
        if (element == null) {
            // Null entries do not appear to need to be closed, and doing so adds an erroneous extra element
            blockBuilder.appendNull();
        } else {
            Type elementType = fieldType.getElementType();
            if (elementType.equals(BIGINT)) {
                Slice slice = generateLongSlice(element);
                blockBuilder.writeEntry(slice, 0, slice.length());
            } else if (elementType.equals(INTEGER)) {
                Slice slice = generateIntegerSlice(element);
                blockBuilder.writeEntry(slice, 0, slice.length());
            } else if (elementType.equals(VARCHAR)) {
                Slice slice = Slices.utf8Slice((String) element);
                blockBuilder.writeEntry(slice, 0, slice.length());
            } else {
                throw new UnsupportedOperationException(
                        String.format("Array elements of type %s are not currently supported", elementType));
            }
        }
    }

    private static Slice generateLongSlice(Object value) throws UncheckedIOException {
        DynamicSliceOutput output = new DynamicSliceOutput(0);
        output.writeLong((Long) value);
        return output.slice();
    }

    private static Slice generateIntegerSlice(Object value) throws UncheckedIOException {
        DynamicSliceOutput output = new DynamicSliceOutput(0);
        output.writeInt((Integer) value);
        return output.slice();
    }
}
