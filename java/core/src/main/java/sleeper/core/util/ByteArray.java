/*
 * Copyright 2012 Facebook, Inc.
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
 *
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
import java.util.Comparator;

/**
 * Code utilised below is a subset of the ByteArray class from facebook.collections.
 *
 * A wrapper object for primitive byte array type.
 * Required as hashCode and equals methods required for usage of type within a HashSet.
 */
public abstract class ByteArray implements Comparable<ByteArray> {

    public static final ByteArrayComparator BYTE_ARRAY_COMPARATOR = new ByteArrayComparator();

    /**
     * Returns the contents of this object as a primitive byte array.
     *
     * @return the byte array
     */
    public abstract byte[] getArray();

    /**
     * Returns the length of the wrapped array.
     *
     * @return the length of array
     */
    public abstract int getLength();

    /**
     * Wraps a primitive byte array.
     *
     * @param  array primitive byte array
     * @return       the wrapped object
     */
    public static ByteArray wrap(byte[] array) {
        return new PureByteArray(array);
    }

    /**
     * Checks equality of two byte arrays.
     *
     * @param  array1 the first ByteArray object
     * @param  array2 the second ByteArray object
     * @return        true if equal
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

    @Override
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

    @Override
    public int hashCode() {
        return getArray() != null ? Arrays.hashCode(getArray()) : 0;
    }
}

/**
 * Declared implementation of PureByteArray to match design for Arrays comparion.
 */
class PureByteArray extends ByteArray {

    private final byte[] array;

    PureByteArray(byte[] array) {
        this.array = array;
    }

    public byte[] getArray() {
        return array;
    }

    public int getLength() {
        return array.length;
    }

    @Override
    public int compareTo(ByteArray o) {
        return BYTE_ARRAY_COMPARATOR.compare(this, o);
    }

    @Override
    public String toString() {
        return "PureByteArray{" +
                "array=" + Arrays.toString(array) +
                '}';
    }

}

/**
 * Comparator for ByteArray type.
 */
class ByteArrayComparator implements Comparator<ByteArray> {
    @Override
    public int compare(ByteArray o1, ByteArray o2) {
        if (o1 == null) {
            if (o2 == null) {
                return 0;
            } else {
                return -1;
            }
        }

        if (o2 == null) {
            return 1;
        }

        if (o1.getArray() == null) {
            if (o2.getArray() == null) {
                return 0;
            } else {
                return -1;
            }
        }

        if (o2.getArray() == null) {
            return 1;
        }

        int array1Length = o1.getLength();
        int array2Length = o2.getLength();

        int length = Math.min(array1Length, array2Length);

        for (int i = 0; i < length; i++) {
            if (o1.getArray()[i] < o2.getArray()[i]) {
                return -1;
            } else if (o1.getArray()[i] > o2.getArray()[i]) {
                return 1;
            }
        }

        if (array1Length < array2Length) {
            return -1;
        } else if (array1Length > array2Length) {
            return 1;
        } else {
            return 0;
        }
    }
}
