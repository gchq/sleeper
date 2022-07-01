package sleeper.trino.utils;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.*;
import sleeper.trino.handle.SleeperColumnHandle;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class SleeperPageBlockUtils {
    /**
     * Read a single object from the specified channel and position in the given page.
     *
     * @param sleeperColumnHandlesInChannelOrder The handles for each column stored in the page, in the order of the
     *                                           channels in the page.
     * @param page                               The page to read from.
     * @param channelNo                          The channel to read.
     * @param positionNo                         The position to read.
     * @return The read object.
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
     * Write an element into a {@link BlockBuilder}. This method is used when columns of type {@link ArrayType} are
     * being processed, which is experimental at present.
     *
     * @param blockBuilder The block to write the elements into.
     * @param elementType  The type of the element to write.
     * @param element      The element itself.
     */
    public static void writeElementToBuilder(BlockBuilder blockBuilder, Type elementType, Object element) {
        if (element == null) {
            // Null entries do not appear to need to be closed, and doing so adds an erroneous extra element
            blockBuilder.appendNull();
        } else {
            if (elementType.equals(BIGINT)) {
                blockBuilder.writeLong((Long) element);
            } else if (elementType.equals(INTEGER)) {
                blockBuilder.writeInt((Integer) element);
            } else if (elementType.equals(VARCHAR)) {
                Slice slice = Slices.utf8Slice((String) element);
                blockBuilder.writeBytes(slice, 0, slice.length());
            } else {
                throw new UnsupportedOperationException(
                        String.format("Array elements of type %s are not currently supported", elementType));
            }
            blockBuilder.closeEntry();
        }
    }
}
