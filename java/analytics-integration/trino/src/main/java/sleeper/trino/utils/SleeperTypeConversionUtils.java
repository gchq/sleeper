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

import io.airlift.slice.Slice;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.VarcharType;

import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.nio.charset.StandardCharsets;

public class SleeperTypeConversionUtils {

    private SleeperTypeConversionUtils() {
    }

    /**
     * Convert a Sleeper type into a Trino type.
     *
     * @param  sleeperType the Sleeper type
     * @return             the Trino type
     */
    public static io.trino.spi.type.Type convertSleeperTypeToTrinoType(sleeper.core.schema.type.Type sleeperType) {
        if (sleeperType instanceof IntType) {
            return IntegerType.INTEGER;
        }
        if (sleeperType instanceof LongType) {
            return BigintType.BIGINT;
        }
        if (sleeperType instanceof StringType) {
            return VarcharType.VARCHAR;
        }
        // Untested types
        //        if (sleeperType instanceof ByteArrayType) {
        //            return VarbinaryType.VARBINARY;
        //        }
        //        if (sleeperType instanceof ListType) {
        //            sleeper.core.schema.type.Type sleeperElementType = ((ListType) sleeperType).getElementType();
        //            return new io.trino.spi.type.ArrayType(convertSleeperTypeToTrinoType(sleeperElementType));
        //        }
        // It should be possible to expose MapType too, but the Trino MapType is complicated.
        throw new UnsupportedOperationException("Sleeper column type " + sleeperType.toString() + " is not handled");
    }

    /**
     * Convert a Trino type into a Sleeper type.
     *
     * @param  trinoType the Trino type
     * @return           the Sleeper type
     */
    public static sleeper.core.schema.type.Type convertTrinoTypeToSleeperType(io.trino.spi.type.Type trinoType) {
        if (trinoType.equals(IntegerType.INTEGER)) {
            return new IntType();
        }
        if (trinoType.equals(BigintType.BIGINT)) {
            return new LongType();
        }
        if (trinoType.equals(VarcharType.VARCHAR)) {
            return new StringType();
        }
        // Untested type
        //        if (trinoType.equals(VarbinaryType.VARBINARY)) {
        //            return ((Slice) trinoObject).byteArray();
        //        }
        // Types which Sleeper considers non-primitive are never used as row keys and should not be used
        throw new UnsupportedOperationException("Trino column type " + trinoType + " is not handled");
    }

    /**
     * Convert an object of a specified Trino type into an object which can be used as a row key in Sleeper.
     *
     * @param  trinoType   the type of the object
     * @param  trinoObject the object to convert
     * @return             the converted object
     */
    public static Object convertTrinoObjectToSleeperRowKeyObject(io.trino.spi.type.Type trinoType, Object trinoObject) {
        if (trinoType.equals(IntegerType.INTEGER)) {
            return trinoObject;
        }
        if (trinoType.equals(BigintType.BIGINT)) {
            return trinoObject;
        }
        if (trinoType.equals(VarcharType.VARCHAR)) {
            return ((Slice) trinoObject).toString(StandardCharsets.UTF_8);
        }
        // Untested type
        //        if (trinoType.equals(VarbinaryType.VARBINARY)) {
        //            return ((Slice) trinoObject).byteArray();
        //        }
        // Types which Sleeper considers non-primitive are never used as row keys and should not be used
        throw new UnsupportedOperationException("Trino column type " + trinoType + " is not handled");
    }
}
