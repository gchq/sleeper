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
package sleeper.core.util;

import java.util.Arrays;

/**
 * Class to provide wrapper object for primitive byte array into an object.
 * Require as hashCode and equals requied for usage of type within a HashSet.
 *
 */
public class ByteArray implements Comparable<ByteArray> {

    private final byte[] array;

    private ByteArray(byte[] array) {
        this.array = array;
    }

    public byte[] getArray() {
        return array;
    }

    public int getLength() {
        return array.length;
    }

    /**
     * Simple method for wrapping primitive type within an object.
     *
     * @param  array primitive byte array
     * @return       wrapped object
     */
    public static ByteArray wrap(byte[] array) {
        return new ByteArray(array);
    }

    @Override
    public int compareTo(ByteArray o) {
        return 0;
        //return BYTE_ARRAY_COMPARATOR.compare(this, o);
    }

    @Override
    public String toString() {
        return "PureByteArray{" +
                "array=" + Arrays.toString(array) +
                '}';
    }

    @Override
    public int hashCode() {
        return array != null ? Arrays.hashCode(array) : 0;
    }

    /**
     * Method for checking equaility.
     *
     * @param  array1 first ByteArray object
     * @param  array2 second ByteArray object
     * @return        true is equal
     */
    public static boolean equals(ByteArray array1, ByteArray array2) {
        //If both null, then they are equal
        if (array1 == null && array2 == null) {
            return true;
        }
        //If one is null, then equality is false
        if (array1 == null || array2 == null) {
            return false;
        }
        // If set, proceed to check via arrays methodology
        return Arrays.equals(array1.getArray(), array2.getArray());
    }

    /**
     * Method for checking equality versus generic object declaration.
     *
     * @param  o object to compare versus
     * @return   true is equal
     */
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ByteArray)) {
            return false;
        }

        final ByteArray that = (ByteArray) o;

        return ByteArray.equals(this, that);
    }

}
